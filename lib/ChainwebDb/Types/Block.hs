{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module ChainwebDb.Types.Block where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Scientific
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Database.Beam
import Database.Beam.Backend.SQL.Row (FromBackendRow)
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Migrate
import Database.Beam.Postgres (Postgres)
import Database.Beam.Postgres.Syntax (PgValueSyntax)
import Database.Beam.Query (HasSqlEqualityCheck)
------------------------------------------------------------------------------
import ChainwebDb.Types.DbHash
------------------------------------------------------------------------------

newtype HashAsNum = HashAsNum { unHashAsNum :: Scientific }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
  deriving newtype (Num)
  deriving newtype (HasSqlValueSyntax PgValueSyntax)
  deriving newtype (FromBackendRow Postgres, HasSqlEqualityCheck Postgres)

instance BeamMigrateSqlBackend be => HasDefaultSqlDataType be HashAsNum where
  defaultSqlDataType _ _ _ = numericType (Just (80, Nothing))

------------------------------------------------------------------------------
data BlockT f = Block
  { _block_creationTime :: C f UTCTime
  , _block_chainId :: C f Int
  , _block_height :: C f Int
  , _block_hash :: C f DbHash
  , _block_parent :: C f DbHash
  , _block_powHash :: C f DbHash
  , _block_payload :: C f DbHash
  , _block_target :: C f HashAsNum
  , _block_weight :: C f HashAsNum
  , _block_epochStart :: C f UTCTime
  , _block_nonce :: C f Word64
  , _block_flags :: C f Word64
  , _block_miner_acc :: C f Text
  , _block_miner_pred :: C f Text }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Block
  (LensFor block_creationTime)
  (LensFor block_chainId)
  (LensFor block_height)
  (LensFor block_hash)
  (LensFor block_parent)
  (LensFor block_powHash)
  (LensFor block_payload)
  (LensFor block_target)
  (LensFor block_weight)
  (LensFor block_epochStart)
  (LensFor block_nonce)
  (LensFor block_flags)
  (LensFor block_miner_acc)
  (LensFor block_miner_pred)
  = tableLenses

type Block = BlockT Identity
type BlockId = PrimaryKey BlockT Identity

-- deriving instance Eq (PrimaryKey BlockT Identity)
-- deriving instance Eq (PrimaryKey BlockT Maybe)
-- deriving instance Eq Block
-- deriving instance Show (PrimaryKey BlockT Identity)
-- deriving instance Show (PrimaryKey BlockT Maybe)
-- deriving instance Show Block
-- deriving instance Show (BlockT Maybe)
-- deriving instance Ord (PrimaryKey BlockT Identity)
-- deriving instance Ord (PrimaryKey BlockT Maybe)

-- instance ToJSON (PrimaryKey BlockT Identity) where
--     toEncoding = genericToEncoding defaultOptions

-- instance FromJSON (PrimaryKey BlockT Identity)

-- instance ToJSON (PrimaryKey BlockT Maybe) where
--     toEncoding = genericToEncoding defaultOptions

-- instance FromJSON (PrimaryKey BlockT Maybe)

-- instance ToJSON (BlockT Identity) where
--     toEncoding = genericToEncoding defaultOptions

-- instance FromJSON (BlockT Identity)

-- instance ToJSON (BlockT Maybe) where
--     toEncoding = genericToEncoding defaultOptions

-- instance FromJSON (BlockT Maybe)

instance Table BlockT where
  data PrimaryKey BlockT f = BlockId (C f DbHash)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = BlockId . _block_hash

unBlockId (BlockId a) = a
