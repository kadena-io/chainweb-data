{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

import           BasePrelude
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Api.NodeInfo
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker (writeBlock)
import           ChainwebDb.Types.Block
import           ChainwebData.Genesis
import           ChainwebData.Types
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import qualified Data.IntSet as S
import qualified Data.List.NonEmpty as NEL
import qualified Data.Pool as P
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres

---

gaps :: Env -> Maybe Double -> IO ()
gaps e rateLimit = do
  cutBS <- queryCut e
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight $ _env_chainsAtHeight e
  work genesisInfo cids pool >>= \case
    Nothing -> printf "[INFO] No gaps detected.\n"
    Just bs -> do
      count <- newIORef 0
      let strat = case rateLimit of
                    Nothing -> Par'
                    Just _ -> Seq
      printf "[INFO] Filling %d gaps\n" (length bs)
      traverseConcurrently_ strat (f count) bs
      final <- readIORef count
      printf "[INFO] Filled in %d missing blocks.\n" final
  where
    pool = _env_dbConnPool e
    genesisInfo = mkGenesisInfo $ _env_nodeInfo e
    delayFunc =
      case rateLimit of
        Nothing -> pure ()
        Just rps -> threadDelay (round $ 1_000_000 / rps)
    f :: IORef Int -> (BlockHeight, Int) -> IO ()
    f count (h, cid) = do
      headersBetween e (ChainId cid, Low h, High h) >>= traverse_ (writeBlock e pool count)
      delayFunc

--queryGaps :: Env -> IO (BlockHeight, Int, Int)
--queryGaps e = P.withResource pool $ \c -> runBeamPostgres c $ do
--  gaps <- runSelectReturningList
--    $ select
--    $ do
--      (chain, height, nextHeight) <-
--        orderBy_ (asc_ . snd) $
--        aggregate_ (\(c,h) -> (group_ (c,h), lead1_ h)) $ do
--          blk <- all_ $ _cddb_blocks database
--          pure (_block_chainId blk, _block_height blk)
--      guard_ (nextHeight - height >. val_ 1)
--  pure $ NEL.nonEmpty pairs >>= filling cids . expanding . grouping

-- with gaps as (select chainid, height, LEAD (height,1) OVER (PARTITION BY chainid ORDER BY height ASC) AS next_height from blocks group by chainid, height) select * from gaps where next_height - height > 1;

-- | TODO: Parametrize chain id with genesis block info so we can mark min height to grep.
--
work
  :: GenesisInfo
  -> [ChainId]
  -> P.Pool Connection
  -> IO (Maybe (NonEmpty (BlockHeight, Int)))
work gi cids pool = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Pull all (chain, height) pairs --
  pairs <- runSelectReturningList
    $ select
    $ orderBy_ (asc_ . fst)
    $ do
      bs <- all_ $ _cddb_blocks database
      pure (_block_height bs, _block_chainId bs)
  -- Determine the missing pairs --
  pure $ NEL.nonEmpty (map changeIntPair pairs) >>= pruning gi . filling cids . expanding . grouping

changeIntPair :: (Int64, Int64) -> (BlockHeight, Int)
changeIntPair (a, b) = (fromIntegral a, fromIntegral b)

grouping :: NonEmpty (BlockHeight, Int) -> NonEmpty (BlockHeight, NonEmpty Int)
grouping = NEL.map (fst . NEL.head &&& NEL.map snd) . NEL.groupWith1 fst

-- | We know that the input is ordered by height. This function fills adds any
-- rows that are missing.
--
-- Written in such a way as to maintain laziness.
--
expanding :: NonEmpty (BlockHeight, NonEmpty Int) -> NonEmpty (BlockHeight, [Int])
expanding (h :| t) = g h :| f (map g t)
  where
    f :: [(BlockHeight, [Int])] -> [(BlockHeight, [Int])]
    f [] = []
    f [a] = [a]
    f (a:b:cs)
      | fst a + 1 /= fst b = a : f (z : b : cs)
      | otherwise = a : f (b : cs)
      where
        z :: (BlockHeight, [Int])
        z = (fst a + 1, [])

    g :: (BlockHeight, NonEmpty Int) -> (BlockHeight, [Int])
    g = second NEL.toList

filling :: [ChainId] -> NonEmpty (BlockHeight, [Int]) -> Maybe (NonEmpty (BlockHeight, Int))
filling cids pairs = fmap sconcat . NEL.nonEmpty . mapMaybe f $ NEL.toList pairs
  where
    chains :: S.IntSet
    chains = S.fromList $ map unChainId cids

    -- | Detect gaps in existing rows.
    f :: (BlockHeight, [Int]) -> Maybe (NonEmpty (BlockHeight, Int))
    f (h, cs) = NEL.nonEmpty . map (h,) . S.toList . S.difference chains $ S.fromList cs

pruning :: GenesisInfo -> Maybe (NonEmpty (BlockHeight, Int)) -> Maybe (NonEmpty (BlockHeight, Int))
pruning gi pairs = NEL.nonEmpty . NEL.filter p =<< pairs
  where
    p :: (BlockHeight, Int) -> Bool
    p (bh, cid) = bh >= genesisHeight (ChainId cid) gi
