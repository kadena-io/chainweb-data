{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.FillTransfers ( fillTransfers ) where

import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)

import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

---

fillTransfers :: Env -> BackfillArgs -> IO ()
fillTransfers env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error $ fromString $ printf "Error querying cut"
      logg Error $ fromString $ show e
    Right cutBS -> fillTransfersCut env args cutBS

fillTransfersCut :: Env -> BackfillArgs -> ByteString -> IO ()
fillTransfersCut = undefined
