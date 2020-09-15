{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
module Chainweb.Data.Test.Backfill
( tests
) where


import Control.Arrow ((&&&))

import Data.Bifunctor
import Data.Foldable (for_)
import Data.Function
import Data.List (sortOn)
import Data.Map.Lazy (Map)
import qualified Data.Map.Lazy as M
import Data.Maybe (mapMaybe)
import qualified Data.Set as S

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.NodeInfo (NodeInfo(..))
import ChainwebData.Backfill
import ChainwebData.Genesis
import ChainwebData.Types

import Test.Tasty
import Test.Tasty.HUnit


tests :: TestTree
tests = testGroup "Backfill tests"
    [ lookupPlanTests
    ]


lookupPlanTests :: TestTree
lookupPlanTests = testGroup "Lookup plan unit tests"
    [ testCase "New/Old Lookup plans should be the same" $ do
      let a = sortOn (\(t,_,_) -> t) $ oldLookupPlan tenChainsData
          b = sortOn (\(t,_,_) -> t) $ lookupPlan genesisInfo tenChainsData
      a @=? b

    , testCase "New/Old lookup plans should be the same for chains 0-9, pre-fork" $ do
      let a = sortOn (\(t,_,_) -> t) $ oldLookupPlan twentyChainsData
          b = sortOn (\(t,_,_) -> t) $ lookupPlan genesisInfo twentyChainsData

      -- Note: old lookup plans don't accommodate genesis info,
      -- backfilling for all chains back to 0, which is wrong.
      -- The new lookup plans do not build plans for pre-genesis
      -- blocks, so will not generate plans for 10-19 because our
      -- test data only fills to block height 1,000.
      --
      -- So we filter to prove consistency on chains 0-9.
      --
      filter (\(ChainId t,_,_) -> t < 10) a @=? b

    , testCase "New lookup plans generate windows at genesis" $ do
      for_ (filter (\ (ChainId c, _,_) -> c > 9) $ lookupPlan genesisInfo genesisData)
        $ \(c,Low l, High h) -> (l == genesisHeight c genesisInfo && h == genesisHeight c genesisInfo) @?
          "lower and upper bounds are fixed on genesis"

    , testCase "New lookup plans generate windows back to genesis" $ do
      for_ (lookupPlan genesisInfo postForkData) $ \(c,Low l,_) ->
        (l >= genesisHeight c genesisInfo) @? "lower bound is less than genesis height"
    ]
  where
    tenChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..9] ]
    twentyChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..19] ]
    genesisData = M.fromList [ (ChainId x, 852_054) | x <- [0..19] ]
    postForkData = M.fromList [ (ChainId x, 853_056) | x <- [0..19] ]


-- -------------------------------------------------------------------- --
-- Global data

genesisInfo :: GenesisInfo
genesisInfo = mkGenesisInfo $ NodeInfo "mainnet01" "0.0" (S.fromList $ fmap ChainId [0..19]) 20 $ Just [(852054, [(12,[13,11,2]),(13,[12,14,3]),(14,[13,15,4]),(15,[14,0,16]),(8,[5,6,3]),(9,[4,6,7]),(10,[11,0,19]),(11,[12,10,1]),(4,[14,9,19]),(5,[8,7,0]),(6,[8,9,1]),(7,[9,5,2]),(0,[15,10,5]),(16,[15,1,17]),(1,[11,6,16]),(17,[16,2,18]),(2,[12,7,17]), (18,[17,3,19]),(3,[13,8,18]),(19,[10,4,18])]), (0, [(8,[9,7,3]),(9,[8,4,5]),(4,[9,1,2]),(5,[9,6,0]),(6,[5,7,1]),(7,[8,6,2]),(0,[5,2,3]),(1,[4,6,3]),(2,[4,7,0]),(3,[8,0,1])])]
