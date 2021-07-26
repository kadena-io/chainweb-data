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
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)

import           Control.Lens (iforM_)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P

import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)

import           System.Logger.Types hiding (logg)

---

fillEvents :: Env -> BackfillArgs -> EventType -> IO ()
fillEvents env args et = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error $ fromString $ printf "[FAIL] Error querying cut"
      logg Error $ fromString $ show e
    Right cutBS -> fillEventsCut env args et cutBS

fillEventsCut :: Env -> BackfillArgs -> EventType -> ByteString -> IO ()
fillEventsCut env args et cutBS = do
  let curHeight = cutMaxHeight cutBS

  counter <- newIORef 0

  when (et == CoinbaseAndTx) $ do
    let startingHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
          "mainnet01" -> 1722500
          "testnet04" -> 1261001
          _ -> error "Chainweb version: Unknown"
    gaps <- getCoinbaseGaps env startingHeight
    mapM_ (logg Debug . fromString . show) gaps
    let numMissingCoinbase = sum $ map (\(_,a,b) -> b - a - 1) gaps
    logg Info $ fromString $ printf "Got %d gaps\n" (length gaps)

    if null gaps
      then do
        logg Info "[INFO] There are no missing coinbase events on any of the chains!"
        exitSuccess
      else do
        logg Info $ fromString $ printf "[INFO] Filling coinbase transactions of %d blocks.\n" numMissingCoinbase
        race_ (progress counter $ fromIntegral numMissingCoinbase) $ do
          forM gaps $ \(chain, low, high) -> do
            -- TODO Maybe make the chunk size configurable
            forM (rangeToDescGroupsOf 100 (Low $ fromIntegral low) (High $ fromIntegral high)) $ \(chunkLow, chunkHigh) -> do
              headersBetween env (ChainId $ fromIntegral chain, chunkLow, chunkHigh) >>= \case
                Left e -> logg Error $ fromString $ printf "[FAIL] ApiError for range %s: %s\n" (show (chunkLow, chunkHigh)) (show e)
                Right [] -> logg Error $ fromString $ printf "[FAIL] headersBetween: %s\n" $ show (chunkLow, chunkHigh)
                Right headers -> do
                  let payloadHashes = M.fromList $ map (\header -> (hashToDbHash $ _blockHeader_payloadHash header, header)) headers
                  payloadWithOutputsBatch env (ChainId $ fromIntegral chain) payloadHashes >>= \case
                    Left e -> do
                      -- TODO Possibly also check for "key not found" message
                      if (apiError_type e == ClientError)
                        then do
                          forM_ (filter (\header -> curHeight - (fromIntegral $ _blockHeader_height header) > 120) headers) $ \header -> do
                            logg Debug $ fromString $ printf "[DEBUG] Setting numEvents to 0 for all transactions with block hash %s\n" (unDbHash $ hashToDbHash $ _blockHeader_hash header)
                            P.withResource pool $ \c ->
                              withTransaction c $ runBeamPostgres c $
                                runUpdate
                                  $ update (_cddb_transactions database)
                                    (\tx -> _tx_numEvents tx <-. val_ (Just 0))
                                    (\tx -> _tx_block tx ==. val_ (BlockId (hashToDbHash $ _blockHeader_hash header)))
                            logg Debug $ fromString $ show e
                        else logg Error $ fromString $ printf "[FAIL] no payloads for header range (%d, %d) on chain %d" (coerce chunkLow :: Int) (coerce chunkHigh :: Int) chain
                    Right bpwos -> do
                      let writePayload current_header bpwo = do
                            let mh = find (\header -> _blockHeader_hash header == _blockHeader_hash current_header) headers
                            case mh of
                              Nothing -> pure ()
                              Just header -> do
                                let allTxsEvents = snd $ mkBlockEvents' (fromIntegral h) (ChainId $ fromIntegral chain) current_hash bpwo
                                    current_hash = hashToDbHash $ _blockHeader_hash current_header
                                    h = _blockHeader_height header
                                    rqKeyEventsMap = allTxsEvents
                                      & mapMaybe (\ev -> fmap (flip (,) 1) (_ev_requestkey ev))
                                      & M.fromListWith (+)

                                P.withResource pool $ \c ->
                                  withTransaction c $ do
                                    runBeamPostgres c $
                                      runInsert
                                        $ insert (_cddb_events database) (insertValues $ fst $ mkBlockEvents' (fromIntegral h) (ChainId $ fromIntegral chain) current_hash bpwo)
                                        $ onConflict (conflictingFields primaryKey) onConflictDoNothing
                                    withSavepoint c $ runBeamPostgres c $
                                      iforM_ rqKeyEventsMap $ \reqKey num_events ->
                                        runUpdate
                                          $ update (_cddb_transactions database)
                                            (\tx -> _tx_numEvents tx <-. val_ (Just num_events))
                                            (\tx -> _tx_requestKey tx ==. val_ (unDbHash reqKey))
                                atomicModifyIORef' counter (\n -> (n+1, ()))
                      forM_ bpwos (uncurry writePayload)
              forM_ delay threadDelay

  where
    logg = _env_logger env
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env

getCoinbaseGaps :: Env -> Int64 -> IO [(Int64, Int64, Int64)]
getCoinbaseGaps env startingHeight = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgresDebug (_env_logger env Debug . fromString) c $ do
  gaps <- runSelectReturningList $ selectWith $ do
    gaps <- selecting $
      withWindow_ (\e -> frame_ (partitionBy_ (_ev_chainid e)) (orderPartitionBy_ $ asc_ $ _ev_height e) noBounds_)
                  (\e w -> (_ev_chainid e, _ev_height e, lead_ (_ev_height e) (val_ (1 :: Int64)) `over_` w))
                  (all_ $ _cddb_events database)
    pure $ orderBy_ (\(cid,a,_) -> (desc_ a, asc_ cid)) $ do
      res@(_,a,b) <- reuse gaps
      guard_ ((b - a) >. val_ 1)
      pure res
  minHeights <- runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_ev_chainid e), min_ (_ev_height e))) (all_ (_cddb_events database))
  let addStart [] = gaps
      addStart ((_,Nothing):ms) = addStart ms
      addStart ((cid,Just m):ms) = if m > startingHeight
                                     then (cid, startingHeight, m) : addStart ms
                                     else addStart ms
  pure $ addStart minHeights

--with gaps as (select chainid, height, LEAD (height,1) OVER (PARTITION BY chainid ORDER BY height ASC) AS next_height from events) select * from gaps where next_height - height > 1;

--    f :: Map ChainId Int64 -> IORef Int -> ChainId -> IO ()
--    f missingCoinbase count chain = do
--      blocks <- getTxMissingEvents chain pool (fromMaybe 100 $ _backfillArgs_eventChunkSize args)
--      forM_ blocks $ \(h,(current_hash,ph)) -> do
--        payloadWithOutputs e (T2 chain ph) >>= \case
--          Nothing -> printf "[FAIL] No payload for chain %d, height %d, ph %s\n"
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
--            Nothing -> printf "[FAIL] No payload for chain %d, height %d, ph %s\n"
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

