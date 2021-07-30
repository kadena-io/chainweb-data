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
import           ChainwebDb.Types.Event
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
import           Data.Text (Text)
import           Data.Tuple.Strict (T2(..))
import           Data.Witherable.Class (wither)


import           System.Logger.Types hiding (logg)

import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction, withSavepoint)

---

backfill :: Env -> BackfillArgs -> IO ()
backfill env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error "Error querying cut"
      logg Info $ fromString $ show e
    Right cutBS -> backfillBlocksCut env args cutBS

backfillBlocksCut :: Env -> BackfillArgs -> ByteString -> IO ()
backfillBlocksCut env args cutBS = do
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight allCids
  counter <- newIORef 0
  mins <- minHeights cids pool
  let count = M.size mins
  if count /= length cids
    then do
      logg Error $ fromString $ printf "%d chains have block data, but we expected %d." count (length cids)
      logg Error $ fromString $ printf "Please run a 'listen' first, and ensure that each chain has a least one block."
      logg Error $ fromString $ printf "That should take about a minute, after which you can rerun 'backfill' separately."
      exitFailure
    else do
      logg Info $ fromString $ printf "Beginning backfill on %d chains." count
      let strat = case delay of
                    Nothing -> Par'
                    Just _ -> Seq
      race_ (progress logg counter $ foldl' (+) 0 mins)
        $ traverseConcurrently_ strat (f counter) $ lookupPlan genesisInfo mins
  where
    delay = _backfillArgs_delayMicros args
    logg = _env_logger env
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
        Left e -> logg Error $ fromString $ printf "ApiError for range %s: %s" (show range) (show e)
        Right [] -> logg Error $ fromString $ printf "headersBetween: %s" $ show range
        Right hs -> traverse_ (writeBlock env pool count) hs
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
