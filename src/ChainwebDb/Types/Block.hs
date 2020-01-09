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

module ChainwebDb.Types.Block where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Time
import           Data.Word
import           Database.Beam
------------------------------------------------------------------------------
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Miner
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data BlockT f = Block
  { _block_id :: C f Int
  , _block_creationTime :: C f UTCTime
  , _block_chainId :: C f Int
  , _block_height :: C f Int
  , _block_hash :: C f DbHash
  , _block_powHash :: C f DbHash
  , _block_target :: C f DbHash
  , _block_weight :: C f DbHash
  , _block_epochStart :: C f UTCTime
  , _block_nonce :: C f Word64
  , _block_miner :: PrimaryKey MinerT f
  } deriving Generic

Block
  (LensFor block_id)
  (LensFor block_creationTime)
  (LensFor block_chainId)
  (LensFor block_height)
  (LensFor block_hash)
  (LensFor block_powHash)
  (LensFor block_target)
  (LensFor block_weight)
  (LensFor block_epochStart)
  (LensFor block_nonce)
  (MinerId (LensFor block_miner))
  = tableLenses

type Block = BlockT Identity
type BlockId = PrimaryKey BlockT Identity

deriving instance Eq (PrimaryKey BlockT Identity)
deriving instance Eq (PrimaryKey BlockT Maybe)
deriving instance Eq Block
deriving instance Show (PrimaryKey BlockT Identity)
deriving instance Show (PrimaryKey BlockT Maybe)
deriving instance Show Block
deriving instance Show (BlockT Maybe)
deriving instance Ord (PrimaryKey BlockT Identity)
deriving instance Ord (PrimaryKey BlockT Maybe)

instance ToJSON (PrimaryKey BlockT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey BlockT Identity)

instance ToJSON (PrimaryKey BlockT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (PrimaryKey BlockT Maybe)

instance ToJSON (BlockT Identity) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (BlockT Identity)

instance ToJSON (BlockT Maybe) where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON (BlockT Maybe)

instance Beamable BlockT

instance Table BlockT where
  data PrimaryKey BlockT f = BlockId (Columnar f Int)
    deriving (Generic, Beamable)
  primaryKey = BlockId . _block_id

blockKeyToInt :: PrimaryKey BlockT Identity -> Int
blockKeyToInt (BlockId k) = k
