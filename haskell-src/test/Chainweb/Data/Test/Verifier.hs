{-# language RecordWildCards #-}
{-# language DerivingStrategies #-}
{-# language GeneralizedNewtypeDeriving #-}
module Chainweb.Data.Test.Verifier
( tests
) where

import Control.Monad
import Data.Tagged (Tagged(..))
import Data.Typeable (Typeable(..))
import System.Directory

import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.Options

import Database.RocksDB.Internal (Options'(..),mkOpts,freeOpts)
import Chainweb.Storage.Table.RocksDB

tests :: TestTree
tests = askOption (testGroup "Verifier lookup test" . findVerifiers)

newtype RocksDBDir = RocksDBDir (Maybe FilePath)
  deriving newtype Show
  deriving Typeable

instance IsOption RocksDBDir where
  defaultValue = RocksDBDir Nothing
  parseValue = Just . RocksDBDir . Just
  optionName = Tagged "rocks-db-dir"
  optionHelp = Tagged "Location of rocks db directory"

findVerifiers :: RocksDBDir -> [TestTree]
findVerifiers (RocksDBDir rocksDBDir) = testFromMaybe rocksDBDir $ \path ->
  withResource (mkOpts modernDefaultOptions) freeOpts $ \opts' ->
    withResource (open opts' path) closeRocksDb test
  where
    testFromMaybe m test = maybe [] (pure . test) m
    open opts' path = do
      Options'{..} <- opts'
      openReadOnlyRocksDb path _optsPtr
    test _iordb = undefined
