{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Chainweb.RichList
( richList
) where


import Control.DeepSeq
import Control.Monad
import Control.Scheduler

import Chainweb.Api.ChainId
import Chainweb.Api.NodeInfo
import Chainweb.Env
import Chainweb.Lookups

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Csv as Csv
import Data.Pool
import qualified Data.Text as T
import qualified Data.Vector as V

import Database.Beam.Postgres (Connection)

import GHC.Generics

import NeatInterpolation

import System.Process

import Text.Printf


richList :: Env -> IO ()
richList cenv = do

    cut <- queryCut cenv

    let bh = fromIntegral $ cutMaxHeight cut
        cmd = (proc "/bin/sh" ["richlist.sh"]) { cwd = Just "./scripts" }

    void $! readCreateProcess cmd []

    consolidate pool
  where
    pool = _env_dbConnPool cenv

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
