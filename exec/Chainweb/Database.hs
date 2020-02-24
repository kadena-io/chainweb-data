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
import ChainwebDb.Types.MinerKey
import ChainwebDb.Types.PubKey
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
  , miners :: f (TableEntity MinerT)
  , pubkeys :: f (TableEntity PubKeyT)
  , minerkeys :: f (TableEntity MinerKeyT) }
  deriving stock (Generic)
  deriving anyclass (Database be)

migratableDb :: CheckedDatabaseSettings Postgres ChainwebDataDb
migratableDb = defaultMigratableDbSettings `withDbModification` dbModification
  { blocks = modifyCheckedTable id checkedTableModification
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
    , _block_miner = MinerId "miner"
    }
  , miners = modifyCheckedTable id checkedTableModification
    { _miner_account = "account"
    , _miner_pred = "pred"
    }
  , transactions = modifyCheckedTable id checkedTableModification
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
    }
  , pubkeys = modifyCheckedTable id checkedTableModification
    { _pubkey_key = "key" }
  , minerkeys = modifyCheckedTable id checkedTableModification
    { _minerKey_miner = MinerId "miner"
    , _minerKey_key = PubKeyId "key"
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
