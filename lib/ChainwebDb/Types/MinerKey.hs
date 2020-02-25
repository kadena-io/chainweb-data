{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}

module ChainwebDb.Types.MinerKey where

import BasePrelude
import ChainwebDb.Types.Block
import ChainwebDb.Types.PubKey
import Database.Beam

---

data MinerKeyT f = MinerKey
  { _minerKey_block :: PrimaryKey BlockT f
  , _minerKey_key :: PrimaryKey PubKeyT f }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type MinerKey = MinerKeyT Identity
type MinerKeyId = PrimaryKey MinerKeyT Identity

-- https://github.com/tathougies/beam/blob/d87120b58373df53f075d92ce12037a98ca709ab/beam-sqlite/examples/Chinook/Schema.hs#L215-L235
instance Table MinerKeyT where
  data PrimaryKey MinerKeyT f = MinerKeyId (PrimaryKey BlockT f) (PrimaryKey PubKeyT f)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = MinerKeyId <$> _minerKey_block <*> _minerKey_key
