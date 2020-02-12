{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module ChainwebDb.Types.Transaction where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text)
import Database.Beam
------------------------------------------------------------------------------
import ChainwebDb.Types.Block
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data TransactionT f = Transaction
  { _tx_chainId :: C f Int
  , _tx_block :: PrimaryKey BlockT f
  , _tx_creationTime :: C f Int
  , _tx_ttl :: C f Int
  , _tx_gasLimit :: C f Int
  -- TODO Reinstate!
  -- See https://github.com/tathougies/beam/issues/431
  -- , _tx_gasPrice :: C f Double
  , _tx_sender :: C f Text
  , _tx_nonce :: C f Text
  , _tx_requestKey :: C f Text
  , _tx_code :: C f Text
  , _tx_rollback :: C f (Maybe Bool)
  , _tx_step :: C f (Maybe Int)
  -- , _tx_data :: C f (Maybe Value)  -- TODO Deal with JSON
  , _tx_proof :: C f (Maybe Text) }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Transaction
  (LensFor tx_chainId)
  (BlockId (LensFor tx_block))
  (LensFor tx_creationTime)
  (LensFor tx_ttl)
  (LensFor tx_gasLimit)
  -- (LensFor tx_gasPrice)
  (LensFor tx_sender)
  (LensFor tx_nonce)
  (LensFor tx_requestKey)
  (LensFor tx_code)
  (LensFor tx_rollback)
  (LensFor tx_step)
  -- (LensFor tx_data)
  (LensFor tx_proof)
  = tableLenses

type Transaction = TransactionT Identity
type TransactionId = PrimaryKey TransactionT Identity

deriving instance Eq (PrimaryKey TransactionT Identity)
deriving instance Eq (PrimaryKey TransactionT Maybe)
deriving instance Eq Transaction
deriving instance Show (PrimaryKey TransactionT Identity)
deriving instance Show (PrimaryKey TransactionT Maybe)
deriving instance Show Transaction
deriving instance Show (TransactionT Maybe)
deriving instance Ord (PrimaryKey TransactionT Identity)
deriving instance Ord (PrimaryKey TransactionT Maybe)

instance ToJSON (PrimaryKey TransactionT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey TransactionT Identity)

instance ToJSON (PrimaryKey TransactionT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey TransactionT Maybe)

instance ToJSON (TransactionT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (TransactionT Identity)

instance ToJSON (TransactionT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (TransactionT Maybe)

instance Table TransactionT where
  data PrimaryKey TransactionT f = TransactionId (C f Text)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = TransactionId . _tx_requestKey
