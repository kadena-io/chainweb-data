{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Chainweb.BackfillTransfers where

import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           ChainwebData.Types
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Transaction

import           Control.Concurrent.Async (race_)
import           Control.Scheduler hiding (traverse_)

import qualified Data.Pool as P

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import qualified Database.Beam.Postgres.Conduit as PC
import           Database.Beam.Postgres.Full
import           Database.Beam.Query.DataTypes ()
import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

-- backfill an empty transfers table (steps)
-- 1. check if transfers table is actually empty. If so, wait until server fills some rows near "top" to start backfill
-- 2. check if events table has any coinbase gaps. If so,  tell user to fill those gaps
-- 3. Fill from last known max height on each chain all the way back to events activation height(s)
backfillTransfersCut :: Env -> BackfillArgs -> IO ()
backfillTransfersCut env args = do

    withDb env (runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_transfers database) $> as_ @Int32 (val_ 1))) >>= \case
      Just False -> do
        logg Error "Run the server command to start filling this table with some entries!"
        exitSuccess
      Just True -> logg Info "transfers table already exists. Great!"
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as transfers);) failed somehow."

    withDb env (runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_events database) $> as_ @Int32 (val_ 1))) >>= \case
      Just True -> logg Info "events table already exists. Great!"
      Just False -> do
        logg Error "events table does not exist"
        exitFailure
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as events);) failed somehow."

    let startingHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
          "mainnet01" -> 1_722_501 :: Int
          "testnet04" -> 1_261_001
          _ -> error "Chainweb version: Unknown"

    minHeights <- withDbDebug env Debug chainMinHeights
    let checkMinHeights =
          maybe False (== _nodeInfo_numChains (_env_nodeInfo env)) . foldr go (Just 0)
        go (_,mh) (Just acc) =
          case mh of
            Just _ ->  Just $ acc + 1
            Nothing -> Nothing
        go _ Nothing = Nothing
    unless (checkMinHeights minHeights) $ do
      logg Error "Make sure transfers table has an entry for every chain id!"
      exitFailure
    let errNothing msg = maybe (error msg) id
        heightMsg = printf "backfillTransfers: Cannot get height: %s"
    mapM_ (\(cid,h) -> logg Info $ fromString $ printf "Filling transfers table on chain %d from height %d to %d." cid startingHeight (errNothing heightMsg h)) minHeights
    let chainRanges = map (\(cid,mh) -> (cid,) $ rangeToDescGroupsOf (fromMaybe 1000 $ _backfillArgs_chunkSize args) (Low startingHeight) (High $ fromIntegral $ fromJust mh)) minHeights
    ref <- newIORef 0
    let strat = case delay of
          Nothing -> Par'
          Just _ -> Seq
    let total = fromIntegral $ sum (subtract (fromIntegral startingHeight) . fromJust . snd <$> minHeights)
    catch (race_ (progress logg ref total) $ traverseConcurrently_ strat (\(cid,l) -> transferInserter ref cid l) chainRanges)
      $ \(e :: SomeException) -> do
          printf "Depending on the error you may need to run backfill for events %s" (show e)
          exitFailure

  where
    delay = _backfillArgs_delayMicros args
    logg = _env_logger env
    pool = _env_dbConnPool env
    transferInserter :: IORef Int -> Int64 -> [(Low,High)] -> IO ()
    transferInserter count cid ranges = forM_ ranges $ \(Low startingHeight, High endingHeight) -> do
        nr <- P.withResource pool $ \c -> withTransaction c $ runBeamPostgres c $ do
          evs <- runSelectReturningList $ select $ eventSelector (fromIntegral startingHeight) (fromIntegral endingHeight) cid
          PC.runInsert c $ insert (_cddb_transfers database) (insertValues evs)
            $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        atomicModifyIORef' count (\c -> (c + fromIntegral nr, ()))

chainMinHeights :: Pg [(Int64, Maybe Int64)]
chainMinHeights = runSelectReturningList $ select $ aggregate_ (\t -> (group_ (_tr_chainid t), min_ (_tr_height t))) (all_ (_cddb_transfers database))

chainMaxHeights :: Pg [(Int64, Maybe Int64)]
chainMaxHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_tr_chainid e), max_ (_tr_height e))) (all_ (_cddb_transfers database))

eventSelector :: Int64 -> Int64 -> Int64 -> Q Postgres ChainwebDataDb s (TransferT (QExpr Postgres s))
eventSelector startingHeight endingHeight chainId = do
    ev <- all_ (_cddb_events database)
    t <- all_ (_cddb_transactions database)
    guard_ $ _ev_height ev <. val_ endingHeight
    guard_ $ _ev_height ev >=. val_ startingHeight
    guard_ $ _ev_chainid ev ==. val_ chainId
    guard_ $ _ev_name ev ==. val_ "TRANSFER"
    guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 0) ==. (val_ "string")
    guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 1) ==. (val_ "string")
    guard_ $
      pgJsonTypeOf (_ev_params ev -># val_ 2) ==. (val_ "number")
      ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. (val_ "number")
      ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. (val_ "number")
    let from_acct = _ev_params ev -># val_ 0
        to_acct = _ev_params ev -># val_ 1
        -- this is ugly, but we have to do things the sql way
        amount = ifThenElse_ (pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. val_ "number")
                (_ev_params ev -># val_ 2 ->$ "decimal")
                 (ifThenElse_ (pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. val_ "number") (_ev_params ev -># val_ 2 ->$ "int")
                        (_ev_params ev -># val_ 2))
    return Transfer
      { _tr_creationtime = _tx_creationTime t
      , _tr_block = _ev_block ev
      , _tr_requestkey = _ev_requestkey ev
      , _tr_chainid = _ev_chainid ev
      , _tr_height = _ev_height ev
      , _tr_idx = _ev_idx ev
      , _tr_modulename = _ev_module ev
      , _tr_from_acct = cast_ from_acct (varchar Nothing)
      , _tr_to_acct = cast_ to_acct (varchar Nothing)
      , _tr_amount =  cast_ amount double
      }
