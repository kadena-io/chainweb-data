{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NumericUnderscores #-}
module ChainwebData.Genesis
( GenesisInfo(..)
, genesisHeight
, mkGenesisInfo
) where


import Data.List
import qualified Data.Map as M
import Data.Maybe (fromMaybe)

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.NodeInfo (NodeInfo(..))


-- | A wrapper around a parsed graph history map from a 'NodeInfo' object
--
newtype GenesisInfo = GenesisInfo
  { getGenesisInfo :: M.Map Int Int
  } deriving newtype (Eq, Ord, Show, Semigroup, Monoid)

-- | Retrieve the genesis height of a given chainid, provided
-- some information about genesis.
--
genesisHeight :: ChainId -> GenesisInfo -> Int
genesisHeight (ChainId c) (GenesisInfo gi) = gi M.! c

-- | Construct genesis information given current nodeinfo
--
mkGenesisInfo :: NodeInfo -> GenesisInfo
mkGenesisInfo = GenesisInfo
    . foldl' go mempty
    . fromMaybe []
    . _nodeInfo_graphs
  where
    go m (bh, adjs) =
      let f Nothing =  Just bh
          f (Just bh')
            | bh > bh' = Just bh'
            | otherwise = Just bh
      in foldl' (\m' (c', _) -> M.alter f c' m') m adjs
