{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.Backfill
( backfill
, lookupPlan
) where


import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Backfill
import           ChainwebData.Genesis
import           ChainwebData.Types
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)
import           Control.Scheduler hiding (traverse_)

import           Control.Lens (iforM_)
import           Data.ByteString.Lazy (ByteString)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import           Data.Tuple.Strict (T2(..))
import           Data.Witherable.Class (wither)

import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)

---

backfill :: Env -> BackfillArgs -> IO ()
backfill env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      putStrLn "[FAIL] Error querying cut"
      print e
    Right cutBS ->
      if _backfillArgs_onlyEvents args
        then backfillEventsCut env args cutBS
        else backfillBlocksCut env args cutBS

backfillBlocksCut :: Env -> BackfillArgs -> ByteString -> IO ()
backfillBlocksCut env args cutBS = do
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight allCids
  counter <- newIORef 0
  mins <- minHeights cids pool
  let count = M.size mins
  if count /= length cids
    then do
      printf "[FAIL] %d chains have block data, but we expected %d.\n" count (length cids)
      printf "[FAIL] Please run a 'listen' first, and ensure that each chain has a least one block.\n"
      printf "[FAIL] That should take about a minute, after which you can rerun 'backfill' separately.\n"
      exitFailure
    else do
      printf "[INFO] Beginning backfill on %d chains.\n" count
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      race_ (progress counter $ foldl' (+) 0 mins)
        $ traverseConcurrently_ strat (f counter) $ lookupPlan genesisInfo mins
  where
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env
    allCids = _env_chainsAtHeight env
    genesisInfo = mkGenesisInfo $ _env_nodeInfo env
    delayFunc =
      case delay of
        Nothing -> pure ()
        Just d -> threadDelay d
    f :: IORef Int -> (ChainId, Low, High) -> IO ()
    f count range = do
      headersBetween env range >>= \case
        Left e -> printf "[FAIL] ApiError for range %s: %s\n" (show range) (show e)
        Right [] -> printf "[FAIL] headersBetween: %s\n" $ show range
        Right hs -> traverse_ (writeBlock env pool count) hs
      delayFunc

backfillEventsCut :: Env -> BackfillArgs -> ByteString -> IO ()
backfillEventsCut env args cutBS = do
  let curHeight = cutMaxHeight cutBS
      cids = atBlockHeight (fromIntegral curHeight) allCids
  counter <- newIORef 0

  printf "[INFO] Backfilling events (curHeight = %d)\n" curHeight
  missingTxs <- countTxMissingEvents pool

  missingCoinbase <- countCoinbaseTxMissingEvents pool coinbaseEventsActivationHeight
  if M.null missingTxs && M.null missingCoinbase
    then do
      printf "[INFO] There are no events to backfill on any of the %d chains!" (length cids)
      exitSuccess
    else do
      let numTxs = foldl' (+) 0 missingTxs
          numCoinbase = foldl' (\s h -> s + (h - fromIntegral coinbaseEventsActivationHeight)) 0 missingCoinbase

      printf "[INFO] Beginning event backfill of %d txs on %d chains.\n" numTxs (M.size missingTxs)
      printf "[INFO] Also beginning event backfill (of coinbase transactions) of %d txs on %d chains.\n" numCoinbase (M.size missingCoinbase)
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      race_ (progress counter $ fromIntegral (numTxs + numCoinbase))
        $ traverseConcurrently_ strat (f (fromIntegral curHeight) missingCoinbase counter) cids

  where
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env
    allCids = _env_chainsAtHeight env
    delayFunc =
      case delay of
        Nothing -> pure ()
        Just d -> threadDelay d
    coinbaseEventsActivationHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
      "mainnet01" -> 1722500
      "testnet04" -> 1261001
      _ -> error "Chainweb version: Unknown"
    f :: Int64 -> Map ChainId Int64 -> IORef Int -> ChainId -> IO ()
    f curHeight missingCoinbase count chain = do
      blocks <- getTxMissingEvents chain pool (fromMaybe 100 $ _backfillArgs_eventChunkSize args)
      putStrLn "[DEBUG] Backfilling tx events"
      forM_ blocks $ \(height,(blockHash,payloadHash)) -> do
        payloadWithOutputs env (T2 chain payloadHash) >>= \case
          Left e -> do
            -- TODO Possibly also check for "key not found" message
            if apiError_type e == ClientError && curHeight - height > 120
              then do
                printf "[DEBUG] Setting numEvents to 0 for all transactions with block hash %s\n" (unDbHash blockHash)
                P.withResource pool $ \c ->
                  withTransaction c $ runBeamPostgres c $
                    runUpdate
                      $ update (_cddb_transactions database)
                          (\tx -> _tx_numEvents tx <-. val_ (Just 0))
                          (\tx -> _tx_block tx ==. val_ (BlockId blockHash))
              else do
                printf "[FAIL] No payload for chain %d, height %d, block hash %s\n"
                       (unChainId chain) height (unDbHash blockHash)
                print e
          Right bpwo -> do
            let allTxsEvents = snd $ mkBlockEvents' height chain blockHash bpwo
                rqKeyEventsMap = allTxsEvents
                  & mapMaybe (\ev -> fmap (flip (,) 1) (_ev_requestkey ev))
                  & M.fromListWith (+)

            P.withResource pool $ \c ->
              withTransaction c $ do
                runBeamPostgres c $ do
                  runInsert
                    $ insert (_cddb_events database) (insertValues allTxsEvents)
                    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
                withSavepoint c $ runBeamPostgres c $
                  iforM_ rqKeyEventsMap $ \reqKey num_events ->
                    runUpdate
                      $ update (_cddb_transactions database)
                          (\tx -> _tx_numEvents tx <-. val_ (Just num_events))
                          (\tx -> _tx_requestKey tx ==. val_ (unDbHash reqKey))
        atomicModifyIORef' count (\n -> (n+1, ()))

      putStrLn "[DEBUG] Backfilling coinbase events"
      forM_ (M.lookup chain missingCoinbase) $ \minheight -> do
        coinbaseBlocks <- getCoinbaseMissingEvents chain (fromIntegral coinbaseEventsActivationHeight) minheight pool
        forM_ coinbaseBlocks $ \(height, (blockHash, payloadHash)) -> do
          payloadWithOutputs env (T2 chain payloadHash) >>= \case
            Left e -> do
              unless (apiError_type e == ClientError && curHeight - height > 120) $ do
                printf "[FAIL] No payload for chain %d, height %d, block hash %s\n"
                       (unChainId chain) height (unDbHash blockHash)
                print e
            Right bpwo -> do
              P.withResource pool $ \c -> runBeamPostgres c $
                runInsert
                  $ insert (_cddb_events database) (insertValues $ fst $ mkBlockEvents' height chain blockHash bpwo)
                  $ onConflict (conflictingFields primaryKey) onConflictDoNothing
          atomicModifyIORef' count (\n -> (n+1, ()))
          forM_ delay threadDelay

      delayFunc

-- | For all blocks written to the DB, find the shortest (in terms of block
-- height) for each chain.
minHeights :: [ChainId] -> P.Pool Connection -> IO (Map ChainId Int)
minHeights cids pool = M.fromList <$> wither selectMinHeight cids
  where
    -- | Get the current minimum height of any block on some chain.
    selectMinHeight :: ChainId -> IO (Maybe (ChainId, Int))
    selectMinHeight cid_@(ChainId cid) = fmap (fmap (\bh -> (cid_, fromIntegral $ _block_height bh)))
      $ P.withResource pool
      $ \c -> runBeamPostgres c
      $ runSelectReturningOne
      $ select
      $ limit_ 1
      $ orderBy_ (asc_ . _block_height)
      $ filter_ (\b -> _block_chainId b ==. val_ (fromIntegral cid))
      $ all_ (_cddb_blocks database)

-- | For all blocks written to the DB, find the shortest (in terms of block
-- height) for each chain.
countTxMissingEvents :: P.Pool Connection -> IO (Map ChainId Int64)
countTxMissingEvents pool = do
    chainCounts <- P.withResource pool $ \c -> runBeamPostgres c $
      runSelectReturningList $
      select $
      aggregate_ agg $ do
        tx <- all_ (_cddb_transactions database)
        blk <- all_ (_cddb_blocks database)
        guard_ (_tx_block tx `references_` blk &&. _tx_numEvents tx ==. val_ Nothing)
        return (_block_chainId blk, _tx_requestKey tx)
    return $ M.fromList $ map (\(cid,cnt) -> (ChainId $ fromIntegral cid, cnt)) chainCounts
  where
    agg (cid, rk) = (group_ cid, as_ @Int64 $ countOver_ distinctInGroup_ rk)

{- We only need to go back so far to search for events in coinbase transactions -}
countCoinbaseTxMissingEvents :: P.Pool Connection -> Integer -> IO (Map ChainId Int64)
countCoinbaseTxMissingEvents pool eventsActivationHeight = do
    chainCounts <- P.withResource pool $ \c -> runBeamPostgresDebug putStrLn c $
      runSelectReturningList $
      select $
      aggregate_ agg $ do
        event <- all_ (_cddb_events database)
        guard_ (_ev_height event >=. val_ (fromIntegral eventsActivationHeight))
        return (_ev_chainid event, _ev_height event)
    return $ M.mapMaybe id $ M.fromList $ map (first $ ChainId . fromIntegral) chainCounts
  where
    agg (cid, height) = (group_ cid, as_ @(Maybe Int64) $ min_ height)

getCoinbaseMissingEvents :: ChainId -> Int64 -> Int64 -> P.Pool Connection -> IO [(Int64, (DbHash, DbHash))]
getCoinbaseMissingEvents chain eventsActivationHeight minHeight pool =
    P.withResource pool $ \c -> runBeamPostgres c $
    runSelectReturningList $
    select $
    orderBy_ (desc_ . fst) $ nub_ $ do
      blk <- all_ (_cddb_blocks database)
      guard_ $ _block_height blk >=. val_ eventsActivationHeight
                &&. _block_height blk <. val_ minHeight
                &&. _block_chainId blk ==. val_ cid
      return $ (_block_height blk, (_block_hash blk, _block_payload blk))
  where
    cid :: Int64
    cid = fromIntegral $ unChainId chain

-- | Get the highest lim blocks with transactions that have unfilled events
getTxMissingEvents
  :: ChainId
  -> P.Pool Connection
  -> Integer
  -> IO [(Int64, (DbHash, DbHash))]
  -- ^ block height, block hash, payload hash
getTxMissingEvents chain pool lim = do
    P.withResource pool $ \c -> runBeamPostgres c $
      runSelectReturningList $
      select $
      limit_ lim $
      orderBy_ (desc_ . fst) $ nub_ $ do
        tx <- all_ (_cddb_transactions database)
        blk <- all_ (_cddb_blocks database)
        guard_ (_tx_block tx `references_` blk &&.
                _tx_numEvents tx ==. val_ Nothing &&.
                _block_chainId blk ==. val_ cid)
        return (_block_height blk, (_block_hash blk, _block_payload blk))
  where
    cid :: Int64
    cid = fromIntegral $ unChainId chain
