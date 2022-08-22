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
import           ChainwebData.Types
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transfer

import           Control.Concurrent.Async (race_)
import           Control.Lens hiding ((<.), reuse)

import qualified Data.Aeson as A
import           Data.Aeson.Lens
import qualified Data.Pool as P
import qualified Data.Text.Read as TR

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full
import           Database.PostgreSQL.Simple

import           System.Logger.Types hiding (logg)

-- backfill an empty transfers table (steps)
-- 1. check if transfers table is actually empty. If so, wait until server fills some rows near "top" to start backfill
-- 2. check if events table has any coinbase gaps. If so,  tell user to fill those gaps
-- 3. Fill from last known max height on each chain all the way back to events activation height(s)
backfillTransfersCut :: Env -> Bool -> BackfillArgs -> IO ()
backfillTransfersCut env _disableIndexesPred args = do

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

    let err = "Chainweb version: Unknown"
        version = _nodeInfo_chainwebVer $ _env_nodeInfo env

    withEventsMinHeight version err $ \eventsActivationHeight -> do
      minHeights <- withDbDebug env Debug chainMinHeights
      let checkMinHeights xs = getSum (foldMap (maybe mempty (const $ Sum 1) . snd) xs) == _nodeInfo_numChains (_env_nodeInfo env)
      unless (checkMinHeights minHeights) $ do
        logg Error "Make sure transfers table has an entry for every chain id!"
        exitFailure
      let maxMinHeights = maximum $ mapMaybe snd $ minHeights
      -- get maximum possible number of entries to fill
      effectiveTotal <- withDbDebug env Debug $ runSelectReturningOne $ select $ bigEventCount maxMinHeights eventsActivationHeight
      unless (isJust effectiveTotal) $ die "Cannot get the number of entries needed to fill transfers table"
      mapM_ (\(cid, h) -> logg Info $ fromString $ printf "Filling transfers table on chain %d from height %d to height %d." cid eventsActivationHeight (fromJust h)) minHeights
      ref <- newIORef 0
      catch
        (race_ (progress logg ref (fromIntegral $ fromJust $ effectiveTotal))
            (forM_ (rangeToDescGroupsOf chunkSize (fromIntegral eventsActivationHeight) (fromIntegral maxMinHeights))
             $ \(Low endingHeight, High startingHeight) ->
                transferInserter ref (fromIntegral startingHeight) (fromIntegral endingHeight)))
        (\(e :: SomeException) -> do
            printf "\nDepending on the error you may need to run backfill for events\n%s\n" (show e)
            exitFailure)
  where
    logg = _env_logger env
    pool = _env_dbConnPool env
    chunkSize = fromMaybe 200 $ _backfillArgs_chunkSize args
    getValidTransfer :: Event -> (Sum Int, [Transfer] -> [Transfer])
    getValidTransfer ev = maybe mempty ((Sum 1, ) . (:)) $ createTransfer ev
    transferInserter :: IORef Int -> Int64 -> Int64 -> IO ()
    transferInserter count startingHeight endingHeight = do
        P.withResource pool $ \c -> withTransaction c $ runBeamPostgres c $ do
          evs <- runSelectReturningList $ select $ eventSelector startingHeight endingHeight
          let (Sum !cnt, tfs) = foldMap getValidTransfer evs
          runInsert $
            insert (_cddb_transfers database) (insertValues (tfs []))
            $ onConflict (conflictingFields primaryKey) onConflictDoNothing
          liftIO $ atomicModifyIORef' count (\cnt' -> (cnt' + cnt, ()))

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
      <*> getAmount (unwrap $ _ev_params ev)
  where
    from_acct = _ev_params ev ^? to unwrap . ix 0 . _String
    to_acct = _ev_params ev ^? to unwrap . ix 1 . _String
    unwrap (PgJSONB a) = a

getAmount :: [A.Value] -> Maybe KDAScientific
getAmount params = fmap KDAScientific $
    (params ^? ix 2 . key "decimal" . _Number)
    <|>
    (params ^? ix 2 . key "decimal" . _String . to TR.rational  . _Right . _1)
    <|>
    (params ^? ix 2 . key "int" . _Number)
    <|>
    (params ^? ix 2 . key "int" . _String . to TR.rational . _Right . _1)
    <|>
    (params ^? ix 2 . _Number)
    <|>
    (params ^? ix 2 . _String . to TR.rational . _Right . _1)

eventSelector' :: Int64 -> Int64 -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector' startingHeight endingHeight = do
    ev <- all_ (_cddb_events database)
    guard_ $ _ev_height ev <=. val_ startingHeight
    guard_ $ _ev_height ev >=. val_ endingHeight
    guard_ $ _ev_name ev ==. val_ "TRANSFER"
    return ev

eventSelector :: Int64 -> Int64 -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector startingHeight endingHeight = orderBy_ getOrder $ eventSelector' startingHeight endingHeight
  where
    getOrder ev = (desc_ $ _ev_height ev, asc_ $ _ev_chainid ev)

bigEventCount :: Int64 -> Int64 -> Q Postgres ChainwebDataDb s (QGenExpr QValueContext Postgres s Int64)
bigEventCount startingHeight endingHeight = aggregate_  (\_ -> as_ @Int64 countAll_) $ eventSelector' startingHeight endingHeight
