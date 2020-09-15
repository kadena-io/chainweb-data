{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}
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

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)
import           Control.Scheduler hiding (traverse_)

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import           Data.Time.Clock.POSIX (getPOSIXTime)
import           Data.Witherable.Class (wither)

import           Database.Beam
import           Database.Beam.Postgres (Connection, runBeamPostgres)

import           System.IO

---

backfill :: Env -> IO ()
backfill e = do
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
      race_ (progress counter mins)
        $ traverseConcurrently_ Par' (f counter) $ lookupPlan genesisInfo mins
  where
    pool = _env_dbConnPool e
    allCids = _env_chainsAtHeight e
    genesisInfo = mkGenesisInfo $ _env_nodeInfo e

    f :: IORef Int -> (ChainId, Low, High) -> IO ()
    f count range = headersBetween e range >>= \case
      [] -> printf "[FAIL] headersBetween: %s\n" $ show range
      hs -> traverse_ (writeBlock e pool count) hs

progress :: IORef Int -> Map ChainId Int -> IO a
progress count mins = do
  start <- getPOSIXTime
  forever $ do
    threadDelay 30_000_000  -- 30 seconds. TODO Make configurable?
    completed <- readIORef count
    now <- getPOSIXTime
    let perc = (100 * fromIntegral completed / fromIntegral total) :: Double
        elapsedMinutes = (now - start) / 60
        blocksPerMinute = (fromIntegral completed / realToFrac elapsedMinutes) :: Double
        estMinutesLeft = floor (fromIntegral (total - completed) / blocksPerMinute) :: Int
        (timeUnits, timeLeft) | estMinutesLeft < 60 = ("minutes" :: String, estMinutesLeft)
                              | otherwise = ("hours", estMinutesLeft `div` 60)
    printf "[INFO] Progress: %d/%d blocks (%.2f%%), ~%d %s remaining at %.0f blocks per minute.\n"
      completed total perc timeLeft timeUnits blocksPerMinute
    hFlush stdout
  where
    total :: Int
    total = foldl' (+) 0 mins

-- | For all blocks written to the DB, find the shortest (in terms of block
-- height) for each chain.
minHeights :: [ChainId] -> P.Pool Connection -> IO (Map ChainId Int)
minHeights cids pool = M.fromList <$> wither selectMinHeight cids
  where
    -- | Get the current minimum height of any block on some chain.
    selectMinHeight :: ChainId -> IO (Maybe (ChainId, Int))
    selectMinHeight cid_@(ChainId cid) = fmap (fmap (\bh -> (cid_, _block_height bh)))
      $ P.withResource pool
      $ \c -> runBeamPostgres c
      $ runSelectReturningOne
      $ select
      $ limit_ 1
      $ orderBy_ (asc_ . _block_height)
      $ filter_ (\b -> _block_chainId b ==. val_ cid)
      $ all_ (_cddb_blocks database)
