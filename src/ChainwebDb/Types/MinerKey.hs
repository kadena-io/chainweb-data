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

module ChainwebDb.Types.MinerKey where

------------------------------------------------------------------------------
import           Data.Aeson
import           Database.Beam
------------------------------------------------------------------------------
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.PubKey
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data MinerKeyT f = MinerKey
  { _minerKey_id :: C f Int
  , _minerKey_miner :: PrimaryKey MinerT f
  , _minerKey_key :: PrimaryKey PubKeyT f
  } deriving Generic

MinerKey
  (LensFor minerKey_id)
  (MinerId (LensFor minerKey_miner))
  (PubKeyId (LensFor minerKey_key))
  = tableLenses

type MinerKey = MinerKeyT Identity
type MinerKeyId = PrimaryKey MinerKeyT Identity

deriving instance Eq (PrimaryKey MinerKeyT Identity)
deriving instance Eq (PrimaryKey MinerKeyT Maybe)
deriving instance Eq MinerKey
deriving instance Show (PrimaryKey MinerKeyT Identity)
deriving instance Show (PrimaryKey MinerKeyT Maybe)
deriving instance Show MinerKey
deriving instance Show (MinerKeyT Maybe)
deriving instance Ord (PrimaryKey MinerKeyT Identity)
deriving instance Ord (PrimaryKey MinerKeyT Maybe)

instance ToJSON (PrimaryKey MinerKeyT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey MinerKeyT Identity)

instance ToJSON (PrimaryKey MinerKeyT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey MinerKeyT Maybe)

instance ToJSON (MinerKeyT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (MinerKeyT Identity)

instance ToJSON (MinerKeyT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (MinerKeyT Maybe)

instance Beamable MinerKeyT

instance Table MinerKeyT where
  data PrimaryKey MinerKeyT f = MinerKeyId (Columnar f Int)
    deriving (Generic, Beamable)
  primaryKey = MinerKeyId . _minerKey_id
