{-# LANGUAGE NumericUnderscores #-}
module ChainwebData.Genesis
( genesisHeight
) where

import Chainweb.Api.ChainId (ChainId(..))

genesisHeight :: ChainId -> Int
genesisHeight (ChainId c)
  | c `elem` [0..9] = 0
  | c `elem` [10..19] = 852_054
  | otherwise = error "chaingraphs larger than 20 are unimplemented"
