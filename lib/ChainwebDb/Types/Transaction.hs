{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module ChainwebDb.Types.Transaction where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Database.Beam
import Database.Beam.Postgres (PgJSONB)
------------------------------------------------------------------------------
import ChainwebDb.Types.Block
import ChainwebDb.Types.DbHash
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data TransactionT f = Transaction
  { _tx_chainId :: C f Int64
  , _tx_block :: PrimaryKey BlockT f
  , _tx_creationTime :: C f UTCTime
  , _tx_ttl :: C f Int64
  , _tx_gasLimit :: C f Int64
  , _tx_gasPrice :: C f Double
  , _tx_sender :: C f Text
  , _tx_nonce :: C f Text
  , _tx_requestKey :: C f (DbHash TxHash)
  , _tx_code :: C f (Maybe Text)
  , _tx_pactId :: C f (Maybe Text)
  , _tx_rollback :: C f (Maybe Bool)
  , _tx_step :: C f (Maybe Int64)
  , _tx_data :: C f (Maybe (PgJSONB Value))
  , _tx_proof :: C f (Maybe Text)

  , _tx_gas :: C f Int64
  , _tx_badResult :: C f (Maybe (PgJSONB Value))
  , _tx_goodResult :: C f (Maybe (PgJSONB Value))
  , _tx_logs :: C f (Maybe Text)
  , _tx_metadata :: C f (Maybe (PgJSONB Value))
  , _tx_continuation :: C f (Maybe (PgJSONB Value))
  , _tx_txid :: C f (Maybe Int64)
  , _tx_numEvents :: C f (Maybe Int64)
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Transaction
  (LensFor tx_chainId)
  (BlockId (LensFor tx_block))
  (LensFor tx_creationTime)
  (LensFor tx_ttl)
  (LensFor tx_gasLimit)
  (LensFor tx_gasPrice)
  (LensFor tx_sender)
  (LensFor tx_nonce)
  (LensFor tx_requestKey)
  (LensFor tx_code)
  (LensFor tx_pactId)
  (LensFor tx_rollback)
  (LensFor tx_step)
  (LensFor tx_data)
  (LensFor tx_proof)
  (LensFor tx_gas)
  (LensFor tx_badResult)
  (LensFor tx_goodResult)
  (LensFor tx_logs)
  (LensFor tx_metadat)
  (LensFor tx_continuation)
  (LensFor tx_txid)
  (LensFor tx_numEvents)
  = tableLenses

type Transaction = TransactionT Identity
type TransactionId = PrimaryKey TransactionT Identity

-- deriving instance Eq (PrimaryKey TransactionT Identity)
-- deriving instance Eq (PrimaryKey TransactionT Maybe)
-- deriving instance Eq Transaction
-- deriving instance Show (PrimaryKey TransactionT Identity)
-- deriving instance Show (PrimaryKey TransactionT Maybe)
-- deriving instance Show Transaction
-- deriving instance Show (TransactionT Maybe)
-- deriving instance Ord (PrimaryKey TransactionT Identity)
-- deriving instance Ord (PrimaryKey TransactionT Maybe)

instance Table TransactionT where
  data PrimaryKey TransactionT f = TransactionId (C f (DbHash TxHash)) (PrimaryKey BlockT f)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey tx = TransactionId (_tx_requestKey tx) (_tx_block tx)
