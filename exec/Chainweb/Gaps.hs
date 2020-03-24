{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

import           BasePrelude
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker (writeBlock)
import           ChainwebDb.Types.Block
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import qualified Data.IntSet as S
import qualified Data.List.NonEmpty as NEL
import qualified Data.Pool as P
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres

---

gaps :: Env -> IO ()
gaps e@(Env _ c _ _ cids) = withPool c $ \pool -> work cids pool >>= \case
  Nothing -> printf "[INFO] No gaps detected."
  Just bs -> do
    count <- newIORef 0
    traverseConcurrently_ Par' (f pool count) bs
    final <- readIORef count
    printf "[INFO] Filled in %d missing blocks.\n" final
  where
    f :: P.Pool Connection -> IORef Int -> (BlockHeight, Int) -> IO ()
    f pool count (h, cid) =
      headersBetween e (ChainId cid, Low h, High h) >>= traverse_ (writeBlock e pool count)

work :: NonEmpty ChainId -> P.Pool Connection -> IO (Maybe (NonEmpty (BlockHeight, Int)))
work cids pool = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Pull all (chain, height) pairs --
  pairs <- runSelectReturningList
    $ select
    $ orderBy_ (asc_ . fst)
    $ do
      bs <- all_ $ blocks database
      pure (_block_height bs, _block_chainId bs)
  -- Determine the missing pairs --
  pure $ NEL.nonEmpty pairs >>= filling cids . expanding . grouping

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

filling :: NonEmpty ChainId -> NonEmpty (BlockHeight, [Int]) -> Maybe (NonEmpty (BlockHeight, Int))
filling cids pairs = fmap sconcat . NEL.nonEmpty . mapMaybe f $ NEL.toList pairs
  where
    chains :: S.IntSet
    chains = S.fromList . map unChainId $ NEL.toList cids

    -- | Detect gaps in existing rows.
    f :: (BlockHeight, [Int]) -> Maybe (NonEmpty (BlockHeight, Int))
    f (h, cs) = NEL.nonEmpty . map (h,) . S.toList . S.difference chains $ S.fromList cs
