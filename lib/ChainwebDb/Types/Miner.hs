{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies       #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module ChainwebDb.Types.Miner where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Database.Beam
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data MinerT f = Miner
  { _miner_id      :: C f Int
  , _miner_account :: C f Text
  , _miner_pred    :: C f Text }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Miner
  (LensFor miner_id)
  (LensFor miner_account)
  (LensFor miner_pred)
  = tableLenses

type Miner = MinerT Identity
type MinerId = PrimaryKey MinerT Identity

deriving instance Eq (PrimaryKey MinerT Identity)
deriving instance Eq (PrimaryKey MinerT Maybe)
deriving instance Eq Miner
deriving instance Show (PrimaryKey MinerT Identity)
deriving instance Show (PrimaryKey MinerT Maybe)
deriving instance Show Miner
deriving instance Show (MinerT Maybe)
deriving instance Ord (PrimaryKey MinerT Identity)
deriving instance Ord (PrimaryKey MinerT Maybe)

instance ToJSON (PrimaryKey MinerT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey MinerT Identity)

instance ToJSON (PrimaryKey MinerT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey MinerT Maybe)

instance ToJSON (MinerT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (MinerT Identity)

instance ToJSON (MinerT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (MinerT Maybe)

instance Table MinerT where
  data PrimaryKey MinerT f = MinerId (Columnar f Int)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = MinerId . _miner_id

minerKeyToInt :: PrimaryKey MinerT Identity -> Int
minerKeyToInt (MinerId k) = k
