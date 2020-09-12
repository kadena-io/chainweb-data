{-# LANGUAGE NumericUnderscores #-}
module ChainwebData.Genesis
( genesisHeight
) where


import qualified Data.Map as M
import Data.Maybe (fromMaybe)

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.NodeInfo (NodeInfo(..))


genesisHeight :: ChainId -> NodeInfo -> Int
genesisHeight (ChainId c) ni =
    fromMaybe 0 $ M.lookup c . foldr go mempty =<< _nodeInfo_graphs ni
  where
    go (bh, adjs) m =
      let f Nothing =  Just bh
          f (Just bh')
            | bh > bh' = Just bh'
            | otherwise = Just bh
      in foldr (\(c', _) m' -> M.alter f c' m') m adjs
