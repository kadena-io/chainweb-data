{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Chainweb.Database
  ( ChainwebDataDb(..)
  , database
  , initializeTables

  , withDb
  , withDbDebug
  ) where

import           Chainweb.Env
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Event
import qualified Data.Pool as P
import           Data.Proxy
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.String
import           Database.Beam
import qualified Database.Beam.AutoMigrate as BA
import           Database.Beam.Postgres
import           System.Exit
import           System.Logger hiding (logg)

---

data ChainwebDataDb f = ChainwebDataDb
  { _cddb_blocks :: f (TableEntity BlockT)
  , _cddb_transactions :: f (TableEntity TransactionT)
  , _cddb_minerkeys :: f (TableEntity MinerKeyT)
  , _cddb_events :: f (TableEntity EventT)
  }
  deriving stock (Generic)
  deriving anyclass (Database be)

modTableName :: Text -> Text
modTableName = T.takeWhileEnd (/= '_')

database :: DatabaseSettings be ChainwebDataDb
database = defaultDbSettings `withDbModification` dbModification
  { _cddb_blocks = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _block_creationTime = "creationtime"
    , _block_chainId = "chainid"
    , _block_height = "height"
    , _block_hash = "hash"
    , _block_parent = "parent"
    , _block_powHash = "powhash"
    , _block_payload = "payload"
    , _block_target = "target"
    , _block_weight = "weight"
    , _block_epochStart = "epoch"
    , _block_nonce = "nonce"
    , _block_flags = "flags"
    , _block_miner_acc = "miner"
    , _block_miner_pred = "predicate"
    }
  , _cddb_transactions = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _tx_chainId = "chainid"
    , _tx_block = BlockId "block"
    , _tx_creationTime = "creationtime"
    , _tx_ttl = "ttl"
    , _tx_gasLimit = "gaslimit"
    , _tx_gasPrice = "gasprice"
    , _tx_sender = "sender"
    , _tx_nonce = "nonce"
    , _tx_requestKey = "requestkey"
    , _tx_code = "code"
    , _tx_pactId = "pactid"
    , _tx_rollback = "rollback"
    , _tx_step = "step"
    , _tx_data = "data"
    , _tx_proof = "proof"

    , _tx_gas = "gas"
    , _tx_badResult = "badresult"
    , _tx_goodResult = "goodresult"
    , _tx_logs = "logs"
    , _tx_metadata = "metadata"
    , _tx_continuation = "continuation"
    , _tx_txid = "txid"
    , _tx_numEvents = "num_events"
    }
  , _cddb_minerkeys = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _minerKey_block = BlockId "block"
    , _minerKey_key = "key"
    }
  , _cddb_events = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _ev_requestkey = "requestkey"
    , _ev_block = "block"
    , _ev_chainid = "chainid"
    , _ev_height = "height"
    , _ev_idx = "idx"
    , _ev_name = "name"
    , _ev_qualName = "qualname"
    , _ev_module = "module"
    , _ev_moduleHash = "modulehash"
    , _ev_paramText = "paramtext"
    , _ev_params = "params"
    }
  }

annotatedDb :: BA.AnnotatedDatabaseSettings be ChainwebDataDb
annotatedDb = BA.defaultAnnotatedDbSettings database

hsSchema :: BA.Schema
hsSchema = BA.fromAnnotatedDbSettings annotatedDb (Proxy @'[])

showMigration :: Connection -> IO ()
showMigration conn =
  runBeamPostgres conn $
    BA.printMigration $ BA.migrate conn hsSchema

-- | Create the DB tables if necessary.
initializeTables :: LogFunctionIO Text -> MigrateStatus -> Connection -> IO ()
initializeTables logg migrateStatus conn = do
  diff <- BA.calcMigrationSteps annotatedDb conn
  case diff of
    Left err -> do
        logg Error "Error detecting database migration requirements: "
        logg Error $ fromString $ show err
    Right [] -> logg Info "No database migration needed.  Continuing..."
    Right _ -> do
      logg Info "Database migration needed."
      case migrateStatus of
        RunMigration -> do
          logg Info "Running the following migration:"
          showMigration conn
          BA.tryRunMigrationsWithEditUpdate annotatedDb conn
        DontMigrate -> do
          logg Info "Database needs to be migrated.  Re-run with the -m option or you can migrate by hand with the following query:"
          showMigration conn
          exitFailure

withDb :: Env -> Pg b -> IO b
withDb env qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgres c qry

withDbDebug :: Env -> LogLevel -> Pg b -> IO b
withDbDebug env level qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgresDebug (liftIO . _env_logger env level . fromString) c qry
