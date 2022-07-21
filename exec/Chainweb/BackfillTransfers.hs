{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
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
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transfer

import           Control.Concurrent.Async (race_)
import           Control.Lens hiding ((<.), reuse)
import           Control.Scheduler hiding (traverse_)

import qualified Data.Aeson as A
import           Data.Aeson.Lens
import qualified Data.Pool as P
import           Data.Scientific (toRealFloat)
import           Data.Text (Text)
import           Data.Time.Clock

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.Beam.Query.DataTypes ()
-- import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

-- backfill an empty transfers table (steps)
-- 1. check if transfers table is actually empty. If so, wait until server fills some rows near "top" to start backfill
-- 2. check if events table has any coinbase gaps. If so,  tell user to fill those gaps
-- 3. Fill from last known max height on each chain all the way back to events activation height(s)
backfillTransfersCut :: Env -> Bool -> BackfillArgs -> IO ()
backfillTransfersCut env disableIndexesPred args = do

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

    let startingHeight :: Int64 = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
          "mainnet01" -> 1_722_501
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
    let maxMinHeights = maximum $ mapMaybe snd $ minHeights
    -- get number of entries that actually need to be filled
    trueTotal <- withDbDebug env Debug $ runSelectReturningOne $ select $ trueTransferCount startingHeight maxMinHeights
    unless (isJust trueTotal) $ die "Cannot get the number of entries needed to fill transfers table"
    mapM_ (\(cid, h) -> logg Info $ fromString $ printf "Filling transfers table on chain %d from height %d to height %d." cid startingHeight (fromJust h)) minHeights

    ref <- newIORef 0
    let _strat = case delay of
          Nothing -> Par'
          Just _ -> Seq
    bool id (withDroppedIndexes pool) disableIndexesPred $
      catch
        (race_ (progress logg ref (fromIntegral $ fromJust $ trueTotal)) $ loopOnJust (transferInserter ref maxMinHeights startingHeight chunkSize) 0)
        $ \(e :: SomeException) -> do
            printf "\nDepending on the error you may need to run backfill for events\n%s\n" (show e)
            exitFailure
  where
    delay = _backfillArgs_delayMicros args
    logg = _env_logger env
    pool = _env_dbConnPool env
    loopOnJust f = go
      where
        go x = do
          mz <- f x
          case mz of
            Just z -> go z
            Nothing -> pure ()
    chunkSize = maybe 1000 fromIntegral $ _backfillArgs_chunkSize args
    getValidTransfer :: Event -> (Sum Int, Min Int64, [Transfer] -> [Transfer])
    getValidTransfer ev = maybe mempty ((Sum 1, Min $ _ev_height ev, ) . (:)) $ createTransfer ev
    transferInserter :: IORef Int -> Int64 -> Int64 -> Integer -> Integer -> IO (Maybe Integer)
    transferInserter count startingHeight endingHeight lim off = trackTime "Transfer actions" logg $ do
        P.withResource pool $ \c -> withTransaction  c $ runBeamPostgres c $ do
          evs <- runSelectReturningList $ select $ eventSelector startingHeight lim off
          let (Sum !cnt, Min !endingHeight', tfs) = foldMap getValidTransfer evs
          runInsert $
            insert (_cddb_transfers database) (insertValues (tfs []))
            $ bool (onConflict (conflictingFields primaryKey) onConflictDoNothing) onConflictDefault disableIndexesPred
          liftIO $ atomicModifyIORef' count (\cnt' -> (cnt' + cnt, ()))
          return $ if endingHeight' <= endingHeight then Nothing else Just $ fromIntegral cnt

trackTime :: String -> LogFunctionIO Text -> IO a -> IO a
trackTime s logg action = do
    t1 <- getCurrentTime
    val <- action
    t2 <- getCurrentTime
    logg Info $ fromString $ printf "%s actions took %s" s (show $ diffUTCTime t2 t1)
    return val

dropIndexes :: P.Pool Connection -> [String] -> IO ()
dropIndexes pool indexnames = forM_ indexnames $ \indexname -> P.withResource pool $ \conn ->
  execute_ conn $ Query $ fromString $ printf "ALTER TABLE transfers DROP CONSTRAINT %s;" indexname

withDroppedIndexes :: P.Pool Connection -> IO a -> IO a
withDroppedIndexes pool action = do
    indexNames <- listTranferIndexes pool
    dropIndexes pool indexNames
    action

listTranferIndexes :: P.Pool Connection -> IO [String]
listTranferIndexes pool = P.withResource pool $ \c -> mapMaybe f <$> query_ c qry
  where
    qry = "SELECT tablename,indexname FROM pg_indexes WHERE schemaname='public';"
    f :: (String,String) -> Maybe String
    f (tblname,indexname)
      | tblname == "transfers" = Just indexname
      | otherwise = Nothing

chainMinHeights :: Pg [(Int64, Maybe Int64)]
chainMinHeights = runSelectReturningList $ select $ aggregate_ (\t -> (group_ (_tr_chainid t), min_ (_tr_height t))) (all_ (_cddb_transfers database))

createTransfer :: Event -> Maybe Transfer
createTransfer ev =
      Transfer
      <$> pure (_ev_block ev)
      <*> pure (_ev_requestkey ev)
      <*> pure (_ev_chainid ev)
      <*> pure (_ev_height ev)
      <*> pure (_ev_idx ev)
      <*> pure (_ev_module ev)
      <*> pure (_ev_moduleHash ev)
      <*> from_acct
      <*> to_acct
      <*> amount
  where
    amount = case _ev_params ev ^? to unwrap . ix 2 . key "decimal" of
      Just (A.Number n) -> Just $ toRealFloat n
      _ -> case _ev_params ev ^? to unwrap . ix 2 . key "int" of
        Just (A.Number n) -> Just $ toRealFloat n
        _ -> fmap toRealFloat $ _ev_params ev ^? to unwrap . ix 2 . _Number
    from_acct = _ev_params ev ^? to unwrap . ix 0 . _String
    to_acct = _ev_params ev ^? to unwrap . ix 1 . _String
    unwrap (PgJSONB a) = a

eventSelector' :: Int64 -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector' startingHeight = do
    ev <- all_ (_cddb_events database)
    guard_ $ _ev_height ev >=. val_ startingHeight
    guard_ $ _ev_name ev ==. val_ "TRANSFER"
    return ev

eventSelector :: Int64 -> Integer -> Integer -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector startingHeight limit offset = limit_ limit $ offset_ offset $ orderBy_ getOrder $ eventSelector' startingHeight
  where
    getOrder ev = (desc_ $ _ev_height ev, asc_ $ _ev_idx ev)

trueTransferCount :: Int64 -> Int64 -> Q Postgres ChainwebDataDb s (QGenExpr QValueContext Postgres s Int64)
trueTransferCount startingHeight endingHeight = aggregate_ (\_ -> as_ @Int64 countAll_) $ do
    ev <- all_ (_cddb_events database)
    guard_ $ foldr1 (&&.)
        [
          _ev_height ev <=. val_ endingHeight
        , _ev_height ev >=. val_ startingHeight
        , _ev_name ev ==. val_ "TRANSFER"
        , pgJsonTypeOf (_ev_params ev -># val_ 0) ==. val_ "string"
        , pgJsonTypeOf (_ev_params ev -># val_ 1) ==. val_ "string"
        , foldr1 (||.)
          [
            pgJsonTypeOf (_ev_params ev -># val_ 2) ==. val_ "number"
          , pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. val_ "number"
          , pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. val_ "number"
          ]
        ]
    return ev
