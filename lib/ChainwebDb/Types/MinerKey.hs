{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}

module ChainwebDb.Types.MinerKey where

import BasePrelude
import ChainwebDb.Types.Miner
import ChainwebDb.Types.PubKey
import Database.Beam

---

data MinerKeyT f = MinerKey
  { _minerKey_id :: C f Int
  , _minerKey_miner :: PrimaryKey MinerT f
  , _minerKey_key :: PrimaryKey PubKeyT f }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type MinerKey = MinerKeyT Identity
type MinerKeyId = PrimaryKey MinerKeyT Identity

instance Table MinerKeyT where
  data PrimaryKey MinerKeyT f = MinerKeyId (C f Int)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = MinerKeyId . _minerKey_id
