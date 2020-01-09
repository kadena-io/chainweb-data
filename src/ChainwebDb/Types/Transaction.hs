{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

module ChainwebDb.Types.Transaction where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Text (Text)
import           Data.Time
import           Database.Beam
------------------------------------------------------------------------------
import           ChainwebDb.Types.Block
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data TransactionT f = Transaction
  { _transaction_id :: C f Int
  , _transaction_chainId :: C f Int
  , _transaction_block :: PrimaryKey BlockT f
  , _transaction_creationTime :: C f UTCTime
  , _transaction_ttl :: C f Int
  , _transaction_gasLimit :: C f Int
  , _transaction_gasPrice :: C f Double
  , _transaction_sender :: C f Text
  , _transaction_nonce :: C f Text
  , _transaction_requestKey :: C f Text
  } deriving Generic

Transaction
  (LensFor transaction_id)
  (LensFor transsaction_chainId)
  (BlockId (LensFor transaction_block))
  (LensFor transaction_creationTime)
  (LensFor transaction_ttl)
  (LensFor transaction_gasLimit)
  (LensFor transaction_gasPrice)
  (LensFor transaction_sender)
  (LensFor transaction_nonce)
  (LensFor transaction_requestKey)
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

instance Beamable TransactionT

instance Table TransactionT where
  data PrimaryKey TransactionT f = TransactionId (Columnar f Int)
    deriving (Generic, Beamable)
  primaryKey = TransactionId . _transaction_id

repoKeyToInt :: PrimaryKey TransactionT Identity -> Int
repoKeyToInt (TransactionId k) = k
