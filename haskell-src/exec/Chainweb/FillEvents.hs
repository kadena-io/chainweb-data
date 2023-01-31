{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.FillEvents
( fillEvents
) where


import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction

import           Control.Concurrent.Async (race_)

import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

---

fillEvents :: Env -> BackfillArgs -> EventType -> IO ()
fillEvents env args et = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error $ fromString $ printf "Error querying cut"
      logg Error $ fromString $ show e
    Right cutBS -> fillEventsCut env args et cutBS

fillEventsCut :: Env -> BackfillArgs -> EventType -> ByteString -> IO ()
fillEventsCut env args et cutBS = do
  let curHeight = cutMaxHeight cutBS
      cids = atBlockHeight (fromIntegral curHeight) allCids

  minHeights <- withDbDebug env Debug chainMinHeights
  let count = length minHeights
  when (count /= length cids) $ do
    logg Error $ fromString $ printf "%d chains have event data, but we expected %d." count (length cids)
    logg Error $ fromString $ printf "Please run 'listen' or 'server' first, and ensure that each chain has a least one event."
    logg Error $ fromString $ printf "That should take about a minute, after which you can rerun this command."
    exitFailure

  counter <- newIORef 0

  when (et == CoinbaseAndTx) $ do
    let version = _nodeInfo_chainwebVer $ _env_nodeInfo env
        err = printf "fillEventsCut failed because we don't know how to work this version %s" version
    withEventsMinHeight version err $ \(startingHeight :: Integer) -> do
      gaps <- getCoinbaseGaps env (fromIntegral startingHeight)
      mapM_ (logg Debug . fromString . show) gaps
      let numMissingCoinbase = sum $ map (\(_,a,b) -> b - a - 1) gaps
      logg Info $ fromString $ printf "Got %d gaps" (length gaps)

      if null gaps
        then do
          logg Info "There are no missing coinbase events on any of the chains!"
          exitSuccess
        else do
          logg Info $ fromString $ printf "Filling coinbase transactions of %d blocks." numMissingCoinbase
          race_ (progress logg counter $ fromIntegral numMissingCoinbase) $ do
            forM gaps $ \(chain, low, high) -> do
              -- TODO Maybe make the chunk size configurable
              forM (rangeToDescGroupsOf 100 (Low $ fromIntegral low) (High $ fromIntegral high)) $ \(chunkLow, chunkHigh) -> do
                headersBetween env (ChainId $ fromIntegral chain, chunkLow, chunkHigh) >>= \case
                  Left e -> logg Error $ fromString $ printf "ApiError for range %s: %s" (show (chunkLow, chunkHigh)) (show e)
                  Right [] -> logg Error $ fromString $ printf "headersBetween: %s" $ show (chunkLow, chunkHigh)
                  Right headers -> do
                    let payloadHashes = M.fromList $ map (\header -> (hashToDbHash $ _blockHeader_payloadHash header, header)) headers
                    payloadWithOutputsBatch env (ChainId $ fromIntegral chain) payloadHashes _blockHeader_hash >>= \case
                      Left e -> do
                        -- TODO Possibly also check for "key not found" message
                        if (apiError_type e == ClientError)
                          then do
                            forM_ (filter (\header -> curHeight - (fromIntegral $ _blockHeader_height header) > 120) headers) $ \header -> do
                              logg Debug $ fromString $ printf "Setting numEvents to 0 for all transactions with block hash %s" (unDbHash $ hashToDbHash $ _blockHeader_hash header)
                              P.withResource pool $ \c ->
                                withTransaction c $ runBeamPostgres c $
                                  runUpdate
                                    $ update (_cddb_transactions database)
                                      (\tx -> _tx_numEvents tx <-. val_ (Just 0))
                                      (\tx -> _tx_block tx ==. val_ (BlockId (hashToDbHash $ _blockHeader_hash header)))
                              logg Debug $ fromString $ show e
                          else logg Error $ fromString $ printf "no payloads for header range (%d, %d) on chain %d" (coerce chunkLow :: Int) (coerce chunkHigh :: Int) chain
                      Right bpwos -> do
                        let write header bpwo = do
                              let curHash = hashToDbHash $ _blockHeader_hash header
                                  height = fromIntegral $ _blockHeader_height header
                              writePayload pool (ChainId $ fromIntegral chain) curHash height (_nodeInfo_chainwebVer $ _env_nodeInfo env) (posixSecondsToUTCTime $ _blockHeader_creationTime header) bpwo
                              atomicModifyIORef' counter (\n -> (n+1, ()))
                        forM_ bpwos (uncurry write)
                forM_ delay threadDelay

  where
    logg = _env_logger env
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env
    allCids = _env_chainsAtHeight env

getCoinbaseGaps :: Env -> Int64 -> IO [(Int64, Int64, Int64)]
getCoinbaseGaps env startingHeight = withDbDebug env Debug $ do
    gaps <- runSelectReturningList $ selectWith $ do
      gaps <- selecting $
        withWindow_ (\e -> frame_ (partitionBy_ (_ev_chainid e)) (orderPartitionBy_ $ asc_ $ _ev_height e) noBounds_)
                    (\e w -> (_ev_chainid e, _ev_height e, lead_ (_ev_height e) (val_ (1 :: Int64)) `over_` w))
                    (all_ $ _cddb_events database)
      pure $ orderBy_ (\(cid,a,_) -> (desc_ a, asc_ cid)) $ do
        res@(_,a,b) <- reuse gaps
        guard_ ((b - a) >. val_ 1)
        pure res
    minHeights <- chainMinHeights
    liftIO $ logg Debug $ fromString $ "minHeights: " <> show minHeights
    let addStart [] = gaps
        addStart ((_,Nothing):ms) = addStart ms
        addStart ((cid,Just m):ms) = if m > startingHeight
                                       then (cid, startingHeight, m) : addStart ms
                                       else addStart ms
    pure $ addStart minHeights
  where
    logg = _env_logger env

chainMinHeights :: Pg [(Int64, Maybe Int64)]
chainMinHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_ev_chainid e), min_ (_ev_height e))) (all_ (_cddb_events database))

--with gaps as (select chainid, height, LEAD (height,1) OVER (PARTITION BY chainid ORDER BY height ASC) AS next_height from events) select * from gaps where next_height - height > 1;

--    f :: Map ChainId Int64 -> IORef Int -> ChainId -> IO ()
--    f missingCoinbase count chain = do
--      blocks <- getTxMissingEvents chain pool (fromMaybe 100 $ _backfillArgs_eventChunkSize args)
--      forM_ blocks $ \(h,(current_hash,ph)) -> do
--        payloadWithOutputs e (T2 chain ph) >>= \case
--          Nothing -> printf "No payload for chain %d, height %d, ph %s"
--                            (unChainId chain) h (unDbHash ph)
--          Just bpwo -> do
--            let allTxsEvents = snd $ mkBlockEvents' h chain current_hash bpwo
--                rqKeyEventsMap = allTxsEvents
--                  & mapMaybe (\ev -> fmap (flip (,) 1) (_ev_requestkey ev))
--                  & M.fromListWith (+)
--
--            P.withResource pool $ \c ->
--              withTransaction c $ do
--                runBeamPostgres c $ do
--                  runInsert
--                    $ insert (_cddb_events database) (insertValues allTxsEvents)
--                    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
--                withSavepoint c $ runBeamPostgres c $
--                  iforM_ rqKeyEventsMap $ \reqKey num_events ->
--                    runUpdate
--                      $ update (_cddb_transactions database)
--                          (\tx -> _tx_numEvents tx <-. val_ (Just num_events))
--                          (\tx -> _tx_requestKey tx ==. val_ (unDbHash reqKey))
--            atomicModifyIORef' count (\n -> (n+1, ()))
--
--      forM_ (M.lookup chain missingCoinbase) $ \minheight -> do
--        coinbaseBlocks <- getCoinbaseMissingEvents chain (fromIntegral coinbaseEventsActivationHeight) minheight pool
--        forM_ coinbaseBlocks $ \(h, (current_hash, ph)) -> do
--          payloadWithOutputs e (T2 chain ph) >>= \case
--            Nothing -> printf "No payload for chain %d, height %d, ph %s"
--                            (unChainId chain) h (unDbHash ph)
--            Just bpwo -> do
--              P.withResource pool $ \c -> runBeamPostgres c $
--                runInsert
--                  $ insert (_cddb_events database) (insertValues $ fst $ mkBlockEvents' h chain current_hash bpwo)
--                  $ onConflict (conflictingFields primaryKey) onConflictDoNothing
--              atomicModifyIORef' count (\n -> (n+1, ()))
--          forM_ delay threadDelay
--
--      delayFunc
--
---- | For all blocks written to the DB, find the shortest (in terms of block
---- height) for each chain.
--countTxMissingEvents :: P.Pool Connection -> IO (Map ChainId Int64)
--countTxMissingEvents pool = do
--    chainCounts <- P.withResource pool $ \c -> runBeamPostgres c $
--      runSelectReturningList $
--      select $
--      aggregate_ agg $ do
--        tx <- all_ (_cddb_transactions database)
--        blk <- all_ (_cddb_blocks database)
--        guard_ (_tx_block tx `references_` blk &&. _tx_numEvents tx ==. val_ Nothing)
--        return (_block_chainId blk, _tx_requestKey tx)
--    return $ M.fromList $ map (\(cid,cnt) -> (ChainId $ fromIntegral cid, cnt)) chainCounts
--  where
--    agg (cid, rk) = (group_ cid, as_ @Int64 $ countOver_ distinctInGroup_ rk)
--
--getCoinbaseMissingEvents :: ChainId -> Int64 -> Int64 -> P.Pool Connection -> IO [(Int64, (DbHash BlockHash, DbHash PayloadHash))]
--getCoinbaseMissingEvents chain eventsActivationHeight minHeight pool =
--    P.withResource pool $ \c -> runBeamPostgres c $
--    runSelectReturningList $
--    select $
--    orderBy_ (desc_ . fst) $ nub_ $ do
--      blk <- all_ (_cddb_blocks database)
--      guard_ $ _block_height blk >=. val_ eventsActivationHeight
--                &&. _block_height blk <. val_ minHeight
--                &&. _block_chainId blk ==. val_ cid
--      return $ (_block_height blk, (_block_hash blk, _block_payload blk))
--  where
--    cid :: Int64
--    cid = fromIntegral $ unChainId chain
--
---- | Get the highest lim blocks with transactions that have unfilled events
--getTxMissingEvents :: ChainId -> P.Pool Connection -> Integer -> IO [(Int64, (DbHash BlockHash, DbHash PayloadHash))]
--getTxMissingEvents chain pool lim = do
--    P.withResource pool $ \c -> runBeamPostgres c $
--      runSelectReturningList $
--      select $
--      limit_ lim $
--      orderBy_ (desc_ . fst) $ nub_ $ do
--        tx <- all_ (_cddb_transactions database)
--        blk <- all_ (_cddb_blocks database)
--        guard_ (_tx_block tx `references_` blk &&.
--                _tx_numEvents tx ==. val_ Nothing &&.
--                _block_chainId blk ==. val_ cid)
--        return (_block_height blk, (_block_hash blk, _block_payload blk))
--  where
--    cid :: Int64
--    cid = fromIntegral $ unChainId chain

