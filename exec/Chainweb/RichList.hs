{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Chainweb.RichList
( richList
) where


import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Scheduler

import Chainweb.Api.ChainId
import Chainweb.Api.NodeInfo
import Chainweb.Env
import Chainweb.Lookups

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Csv as Csv
import Data.Foldable (for_)
import Data.Pool
import qualified Data.Text as T
import qualified Data.Vector as V

import Database.Beam.Postgres (Connection)

import GHC.Generics

import NeatInterpolation

import System.Directory
import System.FilePath
import System.Process

import Text.Printf


richList :: FilePath -> IO ()
richList fp = do
    --
    -- Steps:
    --   1. Check whether specified top-level db path is reachable
    --   2. We assume the node data db is up to date, and for chains 0..19,
    --      Check that the sqlite db paths exist
    --   3. Execute richlist generation, outputing `richlist.csv`
    --
    void $! doesPathExist fp >>= \case
      True -> for_ [0::Int .. 19] $ \i -> do
        let postfix = "chainweb-node/mainnet01/0/sqlite/pact-v1-chain-" <> show i <> ".sqlite"
        let fp' = fp </> postfix

        doesFileExist fp' >>= \case
          True -> return ()
          False -> ioError $ userError $ "Chainweb data not synced. Cannot find sqlite table: " <> fp'
      False -> ioError $ userError $ "Chainweb-node top-level db directory does not exist: " <> fp

    let cmd = (proc "/bin/sh" ["richlist.sh", fp]) { cwd = Just "./scripts" }

    void $! readCreateProcess cmd [] `finally` cleanup
    putStrLn "DONE"
  where
    cleanup = return ()

          -- "> rich-list-chain-" <> c <> ".csv"

-- | TODO: write to postgres. In the meantime, testing with print statements
--
consolidate :: Pool Connection -> IO ()
consolidate _pool = do
    csvData <- LBS.readFile richCsv
    case Csv.decode Csv.NoHeader csvData of
      Left e -> error $ "Error while decoding richlist: " <> show e
      Right (rs :: V.Vector RichAccount) -> print rs
  where
    richCsv = "./scripts/richlist.csv"

-- -------------------------------------------------------------------- --
-- Local data

data RichAccount = RichAccount
    { richAccount :: !String
    , richBalanace :: !Double
    } deriving
      ( Eq, Ord, Show
      , Generic, NFData
      , Csv.ToRecord, Csv.FromRecord
      )
