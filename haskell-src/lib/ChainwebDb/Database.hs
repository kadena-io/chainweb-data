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
{-# LANGUAGE ViewPatterns #-}

module ChainwebDb.Database
  ( ChainwebDataDb(..)
  , database
  , withDb
  , withDbDebug
  ) where

import           ChainwebData.Env
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Signer
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Verifier
import qualified Data.Pool as P
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.String
import           Database.Beam
import           Database.Beam.Postgres
import           System.Logger hiding (logg)

---

data ChainwebDataDb f = ChainwebDataDb
  { _cddb_blocks :: f (TableEntity BlockT)
  , _cddb_transactions :: f (TableEntity TransactionT)
  , _cddb_minerkeys :: f (TableEntity MinerKeyT)
  , _cddb_events :: f (TableEntity EventT)
  , _cddb_signers :: f (TableEntity SignerT)
  , _cddb_transfers :: f (TableEntity TransferT)
  , _cddb_verifiers :: f (TableEntity VerifierT)
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
    { _tx_requestKey = "requestkey"
    , _tx_block = BlockId "block"
    , _tx_chainId = "chainid"
    , _tx_height = "height"
    , _tx_creationTime = "creationtime"
    , _tx_ttl = "ttl"
    , _tx_gasLimit = "gaslimit"
    , _tx_gasPrice = "gasprice"
    , _tx_sender = "sender"
    , _tx_nonce = "nonce"
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
    , _ev_block = BlockId "block"
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
  , _cddb_signers = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _signer_requestkey = "requestkey"
    , _signer_idx = "idx"
    , _signer_pubkey = "pubkey"
    , _signer_scheme = "scheme"
    , _signer_addr = "addr"
    , _signer_caps = "caps"
    , _signer_sig = "sig"
    }
  , _cddb_transfers = modifyEntityName modTableName <>
    modifyTableFields tableModification
    {_tr_requestkey = "requestkey"
    , _tr_chainid = "chainid"
    , _tr_height = "height"
    , _tr_idx = "idx"
    , _tr_modulename = "modulename"
    , _tr_moduleHash = "modulehash"
    , _tr_from_acct = "from_acct"
    , _tr_to_acct = "to_acct"
    , _tr_amount = "amount"
    , _tr_block = BlockId "block"
    }
  , _cddb_verifiers = modifyEntityName modTableName <>
    modifyTableFields tableModification
    { _verifier_requestkey = "requestkey"
    , _verifier_idx = "idx"
    , _verifier_name = "name"
    , _verifier_proof = "proof"
    , _verifier_caps = "caps"
    }
  }

withDb :: Env -> Pg b -> IO b
withDb env qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgres c qry

withDbDebug :: Env -> LogLevel -> Pg b -> IO b
withDbDebug env level qry = P.withResource (_env_dbConnPool env) $ \c -> runBeamPostgresDebug (liftIO . _env_logger env level . fromString) c qry
