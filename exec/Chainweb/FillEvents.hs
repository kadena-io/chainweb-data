{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.FillEvents
( fillEvents
) where


import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common
import           Chainweb.Api.NodeInfo
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
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

import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)

---

fillEvents :: Env -> BackfillArgs -> EventType -> IO ()
fillEvents env args et = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      putStrLn "[FAIL] Error querying cut"
      print e
    Right cutBS -> fillEventsCut env args et cutBS

fillEventsCut :: Env -> BackfillArgs -> EventType -> ByteString -> IO ()
fillEventsCut env args et cutBS = do
  let curHeight = fromIntegral $ cutMaxHeight cutBS
  counter <- newIORef 0

  when (et == CoinbaseAndTx) $ do
    gaps <- getCoinbaseGaps env
    mapM_ print gaps
    let numMissingCoinbase = sum $ map (\(_,a,b) -> b - a - 1) gaps
    printf "Got %d gaps\n" (length gaps)

    if null gaps
      then do
        printf "[INFO] There are no missing coinbase events on any of the chains!"
        exitSuccess
      else do
        printf "[INFO] Filling coinbase transactions of %d blocks.\n" numMissingCoinbase
        race_ (progress counter $ fromIntegral numMissingCoinbase) $ do
          forM gaps $ \(chain, low, high) -> do
            -- TODO Maybe make the chunk size configurable
            forM (rangeToDescGroupsOf 100 low high) $ \(chunkLow, chunkHigh) -> do
              headers <- headersBetween env (chain, chunkLow, chunkHigh)
              let payloadHashes = map (hashToDbHash . _blockHeader_payloadHash) headers
              payloadWithOutputsBatch env chain payloadHashes >>= \case
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
                  P.withResource pool $ \c -> runBeamPostgres c $
                    runInsert
                      $ insert (_cddb_events database) (insertValues $ fst $ mkBlockEvents' h chain current_hash bpwo)
                      $ onConflict (conflictingFields primaryKey) onConflictDoNothing
                  atomicModifyIORef' counter (\n -> (n+1, ()))
              forM_ delay threadDelay
              
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


getCoinbaseGaps :: Env -> IO [(Int64, Int64, Int64)]
getCoinbaseGaps env = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgresDebug putStrLn c $ do
  runSelectReturningList $ selectWith $ do
    gaps <- selecting $
      withWindow_ (\e -> frame_ (partitionBy_ (_ev_chainid e)) (orderPartitionBy_ $ asc_ $ _ev_height e) noBounds_)
                  (\e w -> (_ev_chainid e, _ev_height e, lead_ (_ev_height e) (val_ (1 :: Int64)) `over_` w))
                  (all_ $ _cddb_events database)
    pure $ orderBy_ (\(c,a,_) -> (desc_ a, asc_ c)) $ do
      res@(_,a,b) <- reuse gaps
      guard_ ((b - a) >. val_ 1)
      pure res



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

