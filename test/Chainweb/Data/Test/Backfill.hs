{-# LANGUAGE NumericUnderscores #-}
module Chainweb.Data.Test.Backfill
( tests
) where


import Control.Arrow ((&&&))

import Data.Bifunctor
import Data.Function
import Data.List (sortOn)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe)

import Chainweb.Api.ChainId (ChainId(..))
import ChainwebData.Backfill
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
      (filter (\(ChainId t,_,_) -> t < 10) a) @=? b
    ]
  where
    tenChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..9] ]
    twentyChainsData = M.fromList [ (ChainId x, 1_000) | x <- [0..19] ]
