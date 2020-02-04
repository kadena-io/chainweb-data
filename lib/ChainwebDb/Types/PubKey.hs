{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module ChainwebDb.Types.PubKey where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text)
import Database.Beam
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data PubKeyT f = PubKey
  { _pubkey_id :: C f Int
  , _pubkey_key :: C f Text }
  deriving stock (Generic)
  deriving anyclass (Beamable)

PubKey
  (LensFor pubkey_id)
  (LensFor pubkey_key)
  = tableLenses

type PubKey = PubKeyT Identity
type PubKeyId = PrimaryKey PubKeyT Identity

deriving instance Eq (PrimaryKey PubKeyT Identity)
deriving instance Eq (PrimaryKey PubKeyT Maybe)
deriving instance Eq PubKey
deriving instance Show (PrimaryKey PubKeyT Identity)
deriving instance Show (PrimaryKey PubKeyT Maybe)
deriving instance Show PubKey
deriving instance Show (PubKeyT Maybe)
deriving instance Ord (PrimaryKey PubKeyT Identity)
deriving instance Ord (PrimaryKey PubKeyT Maybe)

instance ToJSON (PrimaryKey PubKeyT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey PubKeyT Identity)

instance ToJSON (PrimaryKey PubKeyT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey PubKeyT Maybe)

instance ToJSON (PubKeyT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PubKeyT Identity)

instance ToJSON (PubKeyT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PubKeyT Maybe)

instance Table PubKeyT where
  data PrimaryKey PubKeyT f = PubKeyId (C f Int)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = PubKeyId . _pubkey_id
