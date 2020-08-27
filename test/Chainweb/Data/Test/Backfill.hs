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
    [ testCase "New/Old Lookup plans should be isomorphic" $ do
      let a = sortOn (\(t,_,_) -> t) $ oldLookupPlan testData
          b = sortOn (\(t,_,_) -> t) $ lookupPlan testData
      a @=? b

    ]
  where
    testData = M.fromList [ (ChainId x, 1_000) | x <- [0..9] ]
