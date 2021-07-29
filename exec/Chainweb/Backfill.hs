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

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
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
import           Control.Concurrent.STM
import           Control.Scheduler hiding (traverse_)

import           Control.Lens (iforM_)
import           Data.ByteString.Lazy (ByteString)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import qualified Data.Vector as V
import           Data.Text (Text)
import           Data.Witherable.Class (wither)


import           System.Logger.Types hiding (logg)

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
      let logg = _env_logger env
      logg Error "Error querying cut"
      logg Info $ fromString $ show e
    Right cutBS ->
      if _backfillArgs_onlyEvents args
        then backfillEventsCut env args cutBS
        else backfillBlocksCut env args cutBS

backfillBlocksCut :: Env -> BackfillArgs -> ByteString -> IO ()
backfillBlocksCut env args cutBS = do
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight allCids
  counter <- newIORef 0
  sampler <- newIORef 0
  mins <- minHeights cids pool
  let count = M.size mins
  if count /= length cids
    then do
      logg Error $ fromString $ printf "%d chains have block data, but we expected %d.\n" count (length cids)
      logg Error $ fromString $ printf "Please run a 'listen' first, and ensure that each chain has a least one block.\n"
      logg Error $ fromString $ printf "That should take about a minute, after which you can rerun 'backfill' separately.\n"
      exitFailure
    else do
      logg Info $ fromString $ printf "Beginning backfill on %d chains.\n" count
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      blockQueue <- newTBQueueIO 20
      race_ (progress env counter $ foldl' (+) 0 mins)
        $ traverseMapConcurrently_ Par' (\k -> traverseConcurrently_ strat (f blockQueue counter sampler) . map (toTriple k)) $ toChainMap $ lookupPlan genesisInfo mins
  where
    traverseMapConcurrently_ comp g m =
      withScheduler_ comp $ \s -> scheduleWork s $ void $ M.traverseWithKey (\k -> scheduleWork s . void . g k) m
    delay = _backfillArgs_delayMicros args
    toTriple k (v1,v2) = (k,v1,v2)
    toChainMap = M.fromListWith (flip (<>)) . map (\(cid,l,h) -> (cid,[(l,h)]))
    logg = _env_logger env
    pool = _env_dbConnPool env
    allCids = _env_chainsAtHeight env
    genesisInfo = mkGenesisInfo $ _env_nodeInfo env
    delayFunc =
      case delay of
        Nothing -> pure ()
        Just d -> threadDelay d
    f :: TBQueue (V.Vector BlockHeader) -> IORef Int -> IORef Int -> (ChainId, Low, High) -> IO ()
    f blockQueue count sampler range = do
      headersBetween env range >>= \case
        Left e -> logg Error $ fromString $ printf "ApiError for range %s: %s\n" (show range) (show e)
        Right [] -> logg Error $ fromString $ printf "headersBetween: %s\n" $ show range
        Right hs -> do
          let vs = V.fromList hs
          atomically (tryReadTBQueue blockQueue) >>= \case
            Nothing -> atomically $ writeTBQueue blockQueue vs
            Just q -> do
              writeBlocks env pool False count sampler (V.toList q)
              atomically $ writeTBQueue blockQueue vs
      delayFunc

backfillEventsCut :: Env -> BackfillArgs -> ByteString -> IO ()
backfillEventsCut env args cutBS = do
  let curHeight = cutMaxHeight cutBS
      cids = atBlockHeight (fromIntegral curHeight) allCids
  counter <- newIORef 0

  logg Info $ fromString $ printf "Backfilling events (curHeight = %d)\n" curHeight
  missingTxs <- countTxMissingEvents pool

  missingCoinbase <- countCoinbaseTxMissingEvents pool logg coinbaseEventsActivationHeight
  if M.null missingTxs && M.null missingCoinbase
    then do
      logg Info $ fromString $ printf "There are no events to backfill on any of the %d chains!" (length cids)
      exitSuccess
    else do
      let numTxs = foldl' (+) 0 missingTxs
          numCoinbase = foldl' (\s h -> s + (h - fromIntegral coinbaseEventsActivationHeight)) 0 missingCoinbase

      logg Info $ fromString $ printf "Beginning event backfill of %d txs on %d chains.\n" numTxs (M.size missingTxs)
      logg Info $ fromString $ printf "Also beginning event backfill (of coinbase transactions) of %d txs on %d chains.\n" numCoinbase (M.size missingCoinbase)
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      race_ (progress env counter $ fromIntegral (numTxs + numCoinbase))
        $ traverseConcurrently_ strat (f (fromIntegral curHeight) missingCoinbase counter) cids

  where
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env
    logg = _env_logger env
    allCids = _env_chainsAtHeight env
    coinbaseEventsActivationHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
      "mainnet01" -> 1722500
      "testnet04" -> 1261001
      _ -> error "Chainweb version: Unknown"
    f :: Int64 -> Map ChainId Int64 -> IORef Int -> ChainId -> IO ()
    f curHeight missingCoinbase count chain = do
      blocks <- getTxMissingEvents chain pool (fromMaybe 100 $ _backfillArgs_eventChunkSize args)
      -- TODO: Make chunk size configurable
      logg Debug "Backfilling tx events"
      forM_ (groupsOf 100 blocks) $ \chunk -> do
        let m = M.fromList $ map (swap . first (either (error hashMsg) id . dbHashToHash) . snd) chunk :: M.Map (DbHash PayloadHash) Hash
            hashMsg = "error converting DbHash to Hash"
        payloadWithOutputsBatch env chain m >>= \case
          Left e -> do
            -- TODO Possibly also check for "key not found" message
            if apiError_type e == ClientError
              then do
                forM_ (filter (\bh -> curHeight - fst bh > 120) blocks) $ \(_, (blockHash, _)) -> do
                  logg Debug $ fromString $ printf "Setting num events to 0 for all transactions with block hash %s\n" (unDbHash blockHash)
                  P.withResource pool $ \c ->
                    withTransaction c $ runBeamPostgres c $
                      runUpdate
                        $ update (_cddb_transactions database)
                            (\tx -> _tx_numEvents tx <-. val_ (Just 0))
                            (\tx -> _tx_block tx ==. val_ (BlockId blockHash))
                logg Info $ fromString $ show e
              else logg Error $ fromString $ printf "No payloads for chain %d, over range (fill in later)" (unChainId chain)
          Right bpwos -> do
                let writePayload current_hash bpwo = do
                      let mh = find (\(_,(c,_)) -> c == hashToDbHash current_hash) blocks
                      case mh of
                        Nothing -> pure ()
                        Just (h,_) -> do
                          let allTxsEvents = snd $ mkBlockEvents' h chain (hashToDbHash current_hash) bpwo
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
                forM_ bpwos (uncurry writePayload)

      logg Debug $ "Backfilling coinbase events"
      forM_ (M.lookup chain missingCoinbase) $ \minheight -> do
        coinbaseBlocks <- getCoinbaseMissingEvents chain (fromIntegral coinbaseEventsActivationHeight) minheight pool
        -- TODO: Make chunk size configurable
        forM_ (groupsOf 100 coinbaseBlocks) $ \chunk -> do
          let m = M.fromList $ map (swap . first (either (error hashMsg) id . dbHashToHash) . snd) chunk
              hashMsg = "error converting DbHash to Hash"
          payloadWithOutputsBatch env chain m >>= \case
            Left e -> do
              unless (apiError_type e == ClientError) $ do
                forM_ (filter (\(height,(_,_)) -> curHeight - height > 120) coinbaseBlocks) $ \(height,(blockHash,_)) -> do
                  logg Error $ fromString $ printf "No payload for chain %d, height %d, block hash %s\n"
                    (unChainId chain) height (unDbHash blockHash)
                  logg Info $ fromString $ show e
              printf "No payloads for chain %d, over range (fill in later)" (unChainId chain)
              print e
            Right bpwos -> do
                let writePayload current_hash bpwo = do
                      let mh = find (\(_,(c,_)) -> c == hashToDbHash current_hash) coinbaseBlocks
                      case mh of
                        Nothing -> pure ()
                        Just (h,_) -> do
                          P.withResource pool $ \c -> runBeamPostgres c $
                            runInsert
                              $ insert (_cddb_events database) (insertValues $ fst $ mkBlockEvents' h chain (hashToDbHash current_hash) bpwo)
                              $ onConflict (conflictingFields primaryKey) onConflictDoNothing
                          atomicModifyIORef' count (\n -> (n+1, ()))
                forM_ bpwos (uncurry writePayload)
          forM_ delay threadDelay

      forM_ delay threadDelay

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
countCoinbaseTxMissingEvents :: P.Pool Connection -> LogFunctionIO Text -> Integer -> IO (Map ChainId Int64)
countCoinbaseTxMissingEvents pool logger eventsActivationHeight = do
    chainCounts <- P.withResource pool $ \c -> runBeamPostgresDebug (logger Debug . fromString) c $
      runSelectReturningList $
      select $
      aggregate_ agg $ do
        event <- all_ (_cddb_events database)
        guard_ (_ev_height event >=. val_ (fromIntegral eventsActivationHeight))
        return (_ev_chainid event, _ev_height event)
    return $ M.mapMaybe id $ M.fromList $ map (first $ ChainId . fromIntegral) chainCounts
  where
    agg (cid, height) = (group_ cid, as_ @(Maybe Int64) $ min_ height)

getCoinbaseMissingEvents :: ChainId -> Int64 -> Int64 -> P.Pool Connection -> IO [(Int64, (DbHash BlockHash, DbHash PayloadHash))]
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
  -> IO [(Int64, (DbHash BlockHash, DbHash PayloadHash))]
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
