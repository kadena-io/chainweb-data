{-# LANGUAGE NumericUnderscores #-}
module ChainwebData.Genesis
( genesisHeight
) where


import qualified Data.Map as M

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.NodeInfo (NodeInfo(..))


genesisHeight :: ChainId -> NodeInfo -> Maybe Int
genesisHeight (ChainId c) ni = _nodeInfo_graphs ni >>= M.lookup c . foldr go mempty
  where
    go (bh, adjs) m =
      let f Nothing =  Just bh
          f (Just bh')
            | bh > bh' = Just bh'
            | otherwise = Just bh
      in foldr (\(c', _) m' -> M.alter f c' m') m adjs
