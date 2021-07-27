{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import qualified Control.Monad.Fail as Fail
import qualified Data.Pool as P
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.String
import           Database.Beam
import           Database.Beam.Migrate
import           Database.Beam.Migrate.Backend
import           Database.Beam.Migrate.Simple
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Migrate (migrationBackend)
import           Database.Beam.Postgres.Syntax
import           System.Logger

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

migratableDb :: CheckedDatabaseSettings Postgres ChainwebDataDb
migratableDb = defaultMigratableDbSettings `withDbModification` dbModification
  { _cddb_blocks = modifyCheckedTable modTableName checkedTableModification
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
  , _cddb_transactions = modifyCheckedTable modTableName checkedTableModification
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
  , _cddb_minerkeys = modifyCheckedTable modTableName checkedTableModification
    { _minerKey_block = BlockId "block"
    , _minerKey_key = "key"
    }
  , _cddb_events = modifyCheckedTable modTableName checkedTableModification
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

database :: DatabaseSettings Postgres ChainwebDataDb
database = unCheckDatabase migratableDb

-- | Create the DB tables if necessary.
initializeTables :: LogFunctionIO Text -> Connection -> IO ()
initializeTables logger conn = runBeamPostgresDebug (logger Debug . fromString) conn $ do
  liftIO $ logger Info "Verifying schema..."
  verifySchema migrationBackend migratableDb >>= \case
    VerificationFailed ps -> do
      liftIO $ do
        logger Info "The following schema predicates need to be satisfied:"
        mapM_ (logger Info . fromString . show) ps
        logger Info "Migrating schema..."
      ourMigrate logger migrationBackend migratableDb
      liftIO $ logger Info "Finished migration."
    VerificationSucceeded -> liftIO $ logger Info "Schema verified."

ourMigrate
  :: forall db m.
     (Database Postgres db,
      Fail.MonadFail m,
      MonadIO m)
  => LogFunctionIO Text
  -> BeamMigrationBackend Postgres m
  -> CheckedDatabaseSettings Postgres db
  -> m ()
ourMigrate logger BeamMigrationBackend { backendActionProvider = actions
                                 , backendGetDbConstraints = getCs }
           db = do
    liftIO $ logger Info "Calling getCs"
    actual <- getCs
    liftIO $ logger Info "Collecting checks"
    let expected = collectChecks db
    liftIO $ logger Info "Running heuristic solver"
    let !solver = heuristicSolver actions actual expected
    case solver of
      ProvideSolution _ -> liftIO $ logger Info "hueristicSolver returned ProvideSolution"
      SearchFailed _ -> liftIO $ logger Info "hueristicSolver returned SearchFailed"
      ChooseActions _ mkac fs _ -> liftIO $ do
        logger Info "hueristicSolver returned ChooseActions"
        mapM_ (logger Info . actionEnglish . mkac) fs
    liftIO $ logger Info "Calculationg final solution"
    case finalSolution solver of
      Candidates {} -> Fail.fail "autoMigrate: Could not determine migration"
      Solved cmds -> do
        -- Check if any of the commands are irreversible
        liftIO $ logger Info "Folding over migration commands"
        case foldMap migrationCommandDataLossPossible cmds of
          MigrationKeepsData -> do
            liftIO $ do
              logger Info "Will run the following migration steps:"
              mapM_ (logger Info . fromString . show . fromPgCommand . migrationCommand) cmds
            mapM_ doStep cmds
          _ -> Fail.fail "autoMigrate: Not performing automatic migration due to data loss"
  where
    doStep :: MigrationCommand Postgres -> m ()
    --doStep c = runNoReturn (migrationCommand c)
    doStep c = do
      let s = migrationCommand c
      liftIO $ do
        logger Info "Executing step:"
        logger Info $ fromString $ show $ fromPgCommand s
      runNoReturn s

withDb :: Env -> Pg b -> IO b
withDb env qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgres c qry

withDbDebug :: Env -> LogLevel -> Pg b -> IO b
withDbDebug env level qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgresDebug (liftIO . _env_logger env level . fromString) c qry
