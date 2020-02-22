{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}

module ChainwebDb.Types.PubKey where

import BasePrelude
import Data.Text (Text)
import Database.Beam

---

newtype PubKeyT f = PubKey { _pubkey_key :: C f Text }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type PubKey = PubKeyT Identity
type PubKeyId = PrimaryKey PubKeyT Identity

instance Table PubKeyT where
  data PrimaryKey PubKeyT f = PubKeyId (C f Text)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = PubKeyId . _pubkey_key
