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

-- https://tathougies.github.io/beam/user-guide/models/#what-about-tables-without-primary-keys
data MinerKeyT f = MinerKey
  { _minerKey_miner :: PrimaryKey MinerT f
  , _minerKey_key :: PrimaryKey PubKeyT f }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type MinerKey = MinerKeyT Identity
type MinerKeyId = PrimaryKey MinerKeyT Identity

instance Table MinerKeyT where
  data PrimaryKey MinerKeyT f = NoId
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey _ = NoId
