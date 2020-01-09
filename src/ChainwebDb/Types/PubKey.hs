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

module ChainwebDb.Types.PubKey where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Text (Text)
import           Database.Beam
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data PubKeyT f = PubKey
  { _pubkey_id :: C f Int
  , _pubkey_key :: C f Text
  } deriving Generic

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

instance Beamable PubKeyT

instance Table PubKeyT where
  data PrimaryKey PubKeyT f = PubKeyId (Columnar f Int)
    deriving (Generic, Beamable)
  primaryKey = PubKeyId . _pubkey_id
