{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Backfill ( backfill ) where

import           BasePrelude hiding (insert, range)
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types (groupsOf)
import           ChainwebDb.Types.Block
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)
import           Control.Scheduler hiding (traverse_)
import qualified Data.List.NonEmpty as NEL
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
backfill e@(Env _ c _ _ cids) = withPool c $ \pool -> do
  cont <- newIORef 0
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
      race_ (progress cont mins)
        $ traverseConcurrently_ Par' (f pool cont) $ lookupPlan mins
  where
    f :: P.Pool Connection -> IORef Int -> (ChainId, Low, High) -> IO ()
    f pool count range = headersBetween e range >>= \case
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
minHeights :: NonEmpty ChainId -> P.Pool Connection -> IO (Map ChainId Int)
minHeights cids pool = M.fromList <$> wither (\cid -> fmap (cid,) <$> f cid) (NEL.toList cids)
  where
    -- | Get the current minimum height of any block on some chain.
    f :: ChainId -> IO (Maybe Int)
    f (ChainId cid) = fmap (fmap _block_height)
      $ P.withResource pool
      $ \c -> runBeamPostgres c
      $ runSelectReturningOne
      $ select
      $ limit_ 1
      $ orderBy_ (asc_ . _block_height)
      $ filter_ (\b -> _block_chainId b ==. val_ cid)
      $ all_ (_cddb_blocks database)

-- | Based on some initial minimum heights per chain, form a lazy list of block
-- ranges that need to be looked up.
lookupPlan :: Map ChainId Int -> [(ChainId, Low, High)]
lookupPlan mins = concatMap (\pair -> mapMaybe (g pair) asList) ranges
  where
    maxi :: Int
    maxi = max 0 $ maximum (M.elems mins) - 1

    asList :: [(ChainId, High)]
    asList = map (second (\n -> High . max 0 $ n - 1)) $ M.toList mins

    ranges :: [(Low, High)]
    ranges = map (Low . last &&& High . head) $ groupsOf 100 [maxi, maxi-1 .. 0]

    g :: (Low, High) -> (ChainId, High) -> Maybe (ChainId, Low, High)
    g (l@(Low l'), u) (cid, mx@(High mx'))
      | u > mx && l' <= mx' = Just (cid, l, mx)
      | u <= mx = Just (cid, l, u)
      | otherwise = Nothing
