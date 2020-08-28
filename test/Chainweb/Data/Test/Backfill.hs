{-# LANGUAGE NumericUnderscores #-}
module Chainweb.Data.Test.Backfill
( tests
) where


import Control.Arrow ((&&&))

import Data.Bifunctor
import Data.Foldable (for_)
import Data.Function
import Data.List (sortOn)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe)

import Chainweb.Api.ChainId (ChainId(..))
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
          b = sortOn (\(t,_,_) -> t) $ lookupPlan tenChainsData
      a @=? b

    , testCase "New/Old lookup plans should be the same for chains 0-9, pre-fork" $ do
      let a = sortOn (\(t,_,_) -> t) $ oldLookupPlan twentyChainsData
          b = sortOn (\(t,_,_) -> t) $ lookupPlan twentyChainsData

      -- note: old lookup plans don't accommodate genesis info,
      -- and so backfill for more chains back to 0, which is wrong.
      -- So we filter.
      --
      filter (\(ChainId t,_,_) -> t < 10) a @=? b

    , testCase "New lookup plans generate windows at genesis" $ do
      for_ (filter (\ (ChainId c, _,_) -> c > 9) $ lookupPlan genesisData)
        $ \(c,Low l, High h) -> (l == genesisHeight c && h == genesisHeight c) @?
          "lower and upper bounds are fixed on genesis"

    , testCase "New lookup plans generate windows back to genesis" $ do
      for_ (lookupPlan postForkData) $ \(c,Low l,_) ->
        (l >= genesisHeight c) @? "lower bound is less than genesis height"
    ]
  where
    tenChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..9] ]
    twentyChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..19] ]
    genesisData = M.fromList [ (ChainId x, 852_054) | x <- [0..19] ]
    postForkData = M.fromList [ (ChainId x, 853_056) | x <- [0..19] ]
