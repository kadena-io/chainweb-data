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
{-# OPTIONS_GHC -fno-warn-orphans #-}

module ChainwebDb.Types.Transfer where

----------------------------------------------------------------------------
import BasePrelude
import Data.Scientific
import Data.Text (Text)
import Database.Beam
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Postgres
import Database.Beam.Postgres.Syntax
import qualified Database.Beam.AutoMigrate as BA
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.FromField
------------------------------------------------------------------------------
import ChainwebDb.Types.Block
import ChainwebDb.Types.Common
------------------------------------------------------------------------------
data TransferT f = Transfer
  { _tr_block :: PrimaryKey BlockT f
  , _tr_requestkey :: C f ReqKeyOrCoinbase
  , _tr_chainid :: C f Int64
  , _tr_height :: C f Int64
  , _tr_idx :: C f Int64
  , _tr_modulename :: C f Text
  , _tr_moduleHash :: C f Text
  , _tr_from_acct :: C f Text
  , _tr_to_acct :: C f Text
  , _tr_amount :: C f KDAScientific
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Transfer
  (BlockId (LensFor tr_block))
  (LensFor tr_requestkey)
  (LensFor tr_chainid)
  (LensFor tr_height)
  (LensFor tr_idx)
  (LensFor tr_modulename)
  (LensFor tr_moduleHash)
  (LensFor tr_from_acct)
  (LensFor tr_to_acct)
  (LensFor tr_amount)
 = tableLenses

type Transfer = TransferT Identity
type TransferId = PrimaryKey TransferT Identity

instance Table TransferT where
  data PrimaryKey TransferT f = TransferId (PrimaryKey BlockT f) (C f ReqKeyOrCoinbase) (C f Int64) (C f Int64) (C f Text)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = TransferId <$> _tr_block <*> _tr_requestkey <*> _tr_chainid <*> _tr_idx <*> _tr_moduleHash

newtype KDAScientific = KDAScientific { getKDAScientific :: Scientific }
  deriving newtype (Eq, Show, HasSqlValueSyntax PgValueSyntax, ToField, FromField, FromBackendRow Postgres)

instance BA.HasColumnType KDAScientific where
  defaultColumnType _ = BA.SqlStdType (numericType Nothing)
  defaultTypeCast _ = Just "numeric"
