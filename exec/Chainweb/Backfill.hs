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
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Transaction

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)
import           Control.Scheduler hiding (traverse_)

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import           Data.Witherable.Class (wither)

import           Database.Beam
import           Database.Beam.Postgres

---

backfill :: Env -> BackfillArgs -> IO ()
backfill e args = do
  if _backfillArgs_onlyEvents args
    then backfillBlocks e args
    else backfillEvents e args

backfillBlocks :: Env -> BackfillArgs -> IO ()
backfillBlocks e args = do
  cutBS <- queryCut e
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
    pool = _env_dbConnPool e
    allCids = _env_chainsAtHeight e
    genesisInfo = mkGenesisInfo $ _env_nodeInfo e
    delayFunc =
      case delay of
        Nothing -> pure ()
        Just d -> threadDelay d
    f :: IORef Int -> (ChainId, Low, High) -> IO ()
    f count range = do
      headersBetween e range >>= \case
        [] -> printf "[FAIL] headersBetween: %s\n" $ show range
        hs -> traverse_ (writeBlock e pool count) hs
      delayFunc

backfillEvents :: Env -> BackfillArgs -> IO ()
backfillEvents e args = do
  cutBS <- queryCut e
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight allCids
  counter <- newIORef 0

  missing <- countTxMissingEvents pool
  let count = M.size missing
  if count /= length cids
    then do
      printf "[FAIL] %d chains have tx data, but we expected %d.\n" count (length cids)
      printf "[FAIL] Please run a 'listen' first, and ensure that each chain has a least one block.\n"
      printf "[FAIL] That should take about a minute, after which you can rerun 'backfill' separately.\n"
      exitFailure
    else do
      printf "[INFO] Beginning event backfill on %d chains.\n" count
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      race_ (progress counter $ fromIntegral $ foldl' (+) 0 missing)
        $ traverseConcurrently_ strat (f counter) cids
  where
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool e
    allCids = _env_chainsAtHeight e
    delayFunc =
      case delay of
        Nothing -> pure ()
        Just d -> threadDelay d
    f :: IORef Int -> ChainId -> IO ()
    f count chain = do
      heights <- getTxMissingEvents chain pool 100
      forChunks heights $ \(h,l) -> do
        let range = (chain, Low $ fromIntegral l, High $ fromIntegral h)
        headersBetween e range >>= \case
          [] -> printf "[FAIL] headersBetween: %s\n" $ show range
          hs -> traverse_ (writeBlock e pool count) hs
      delayFunc

forChunks :: Monad m => [Int64] -> ((Int64,Int64) -> m ()) -> m ()
forChunks xs f = do
  case getConsecutiveDesc xs of
    Nothing -> return ()
    Just ((h,l), rest) -> do
      f (h,l)
      forChunks rest f

getConsecutiveDesc :: Integral a => [a] -> Maybe ((a,a), [a])
getConsecutiveDesc [] = Nothing
getConsecutiveDesc [x] = Just ((x,x),[])
getConsecutiveDesc (x:xs) = go x 0 xs
  where
    go _ _ [] = Nothing
    go runStart runLen as@(a:rest)
      | a == runStart - runLen - 1 = go runStart (runLen+1) rest
      | otherwise = Just ((runStart, runStart - runLen), as)


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
    chainCounts <- P.withResource pool $ \c -> runBeamPostgresDebug putStrLn c $
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

-- | Get the highest lim blocks with transactions that have unfilled events
getTxMissingEvents :: ChainId -> P.Pool Connection -> Integer -> IO [Int64]
getTxMissingEvents chain pool lim = do
    heights <- P.withResource pool $ \c -> runBeamPostgresDebug putStrLn c $
      runSelectReturningList $
      select $
      limit_ lim $
      orderBy_ desc_ $ do
        tx <- all_ (_cddb_transactions database)
        blk <- all_ (_cddb_blocks database)
        guard_ (_tx_block tx `references_` blk &&.
                _tx_numEvents tx ==. val_ Nothing &&.
                _block_chainId blk ==. val_ cid)
        return (_block_height blk)
    return heights
  where
    cid :: Int64
    cid = fromIntegral $ unChainId chain
