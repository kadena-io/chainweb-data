{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}

module Chainweb.Database where

import ChainwebDb.Types.Block
import ChainwebDb.Types.Header
import ChainwebDb.Types.Miner
import ChainwebDb.Types.Transaction
import Database.Beam
import Database.Beam.Migrate
import Database.Beam.Migrate.Simple
import Database.Beam.Sqlite (Sqlite, runBeamSqlite)
import Database.Beam.Sqlite.Migrate (migrationBackend)
import Database.SQLite.Simple (Connection)

---

data ChainwebDataDb f = ChainwebDataDb
  { headers :: f (TableEntity HeaderT)
  , blocks :: f (TableEntity BlockT)
  , transactions :: f (TableEntity TransactionT)
  , miners :: f (TableEntity MinerT) }
  deriving stock (Generic)
  deriving anyclass (Database be)

dbSettings :: CheckedDatabaseSettings Sqlite ChainwebDataDb
dbSettings = defaultMigratableDbSettings

-- | Create the DB tables if necessary.
initializeTables :: Connection -> IO ()
initializeTables conn = runBeamSqlite conn $
  verifySchema migrationBackend dbSettings >>= \case
    VerificationFailed _ -> createSchema migrationBackend dbSettings
    VerificationSucceeded -> pure ()
