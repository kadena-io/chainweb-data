{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

import           BasePrelude
import           Chainweb.Api.ChainId
import           Chainweb.Api.Common
import           Chainweb.Database
import           ChainwebDb.Types.Block
import qualified Data.IntSet as S
import qualified Data.List.NonEmpty as NEL
import qualified Data.Pool as P
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres

---

gaps :: P.Pool Connection -> IO (Maybe (NonEmpty (BlockHeight, ChainId)))
gaps pool = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Pull all (chain, height) pairs --
  pairs <- runSelectReturningList
    $ select
    $ orderBy_ (asc_ . fst)
    $ do
      bs <- all_ $ blocks database
      pure (_block_height bs, _block_chainId bs)
  -- Determine the missing pairs --
  case NEL.nonEmpty pairs of
    Nothing -> pure Nothing
    Just ps -> do
      let !chains = S.fromList [0..9]
          !grouped = NEL.groupWith1 fst ps
      pure . NEL.nonEmpty $ concatMap (f chains) grouped
  where
    f :: S.IntSet -> NonEmpty (BlockHeight, Int) -> [(BlockHeight, ChainId)]
    f chains ps =
      map (,cid) . S.toList . S.difference chains . S.fromList . map snd $ NEL.toList ps
      where
        cid :: ChainId
        cid = ChainId . snd $ NEL.head ps

-- moreWork :: P.Pool Connection -> NonEmpty (BlockHeight, ChainId) -> IO ()
-- moreWork pool pairs = do
  -- Perform concurrent block lookups --
  -- Write the new blocks --
  -- undefined
