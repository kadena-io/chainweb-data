{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}

module Chainweb.Database
  ( ChainwebDataDb(..)
  , database
  , initializeTables
  ) where

import BasePrelude
import ChainwebDb.Types.Block
import ChainwebDb.Types.Miner
import ChainwebDb.Types.Transaction
import Database.Beam
import Database.Beam.Migrate
import Database.Beam.Migrate.Simple
import Database.Beam.Postgres (Connection, Postgres, runBeamPostgres)
import Database.Beam.Postgres.Migrate (migrationBackend)

---

data ChainwebDataDb f = ChainwebDataDb
  { blocks :: f (TableEntity BlockT)
  , transactions :: f (TableEntity TransactionT)
  , miners :: f (TableEntity MinerT) }
  deriving stock (Generic)
  deriving anyclass (Database be)

migratableDb :: CheckedDatabaseSettings Postgres ChainwebDataDb
migratableDb = defaultMigratableDbSettings

database :: DatabaseSettings Postgres ChainwebDataDb
database = unCheckDatabase migratableDb

-- | Create the DB tables if necessary.
initializeTables :: Connection -> IO ()
initializeTables conn = runBeamPostgres conn $
  verifySchema migrationBackend migratableDb >>= \case
    VerificationFailed _ -> autoMigrate migrationBackend migratableDb
    VerificationSucceeded -> pure ()
