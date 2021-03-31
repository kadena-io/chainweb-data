{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Chainweb.Database
  ( ChainwebDataDb(..)
  , database
  , initializeTables
  ) where

import           BasePrelude
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Event
import           Data.Text (Text)
import qualified Data.Text as T
import           Database.Beam
import           Database.Beam.Migrate
import           Database.Beam.Migrate.Simple
import           Database.Beam.Postgres (Connection, Postgres, runBeamPostgres)
import           Database.Beam.Postgres.Migrate (migrationBackend)

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
    }
  , _cddb_minerkeys = modifyCheckedTable modTableName checkedTableModification
    { _minerKey_block = BlockId "block"
    , _minerKey_key = "key"
    }
  , _cddb_events = modifyCheckedTable modTableName checkedTableModification
    { _ev_requestKey = TransactionId "requestkey"
    , _ev_idx = "idx"
    , _ev_name = "name"
    , _ev_qualName = "qualName"
    , _ev_module = "module"
    , _ev_moduleHash = "moduleHash"
    , _ev_paramText = "paramText"
    , _ev_params = "params"
    }
  }

database :: DatabaseSettings Postgres ChainwebDataDb
database = unCheckDatabase migratableDb

-- | Create the DB tables if necessary.
initializeTables :: Connection -> IO ()
initializeTables conn = runBeamPostgres conn $
  verifySchema migrationBackend migratableDb >>= \case
    VerificationFailed _ -> autoMigrate migrationBackend migratableDb
    VerificationSucceeded -> pure ()
