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
import Data.Decimal
import Data.Text (Text)
import Database.Beam
import Database.Beam.AutoMigrate.Types hiding (Table)
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
  , _tr_amount :: C f KDADecimal
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

{-

The two values in given to the function numericType correspond to the precision
and scale of the numeric type. Read here for more information
https://www.postgresql.org/docs/current/datatype-numeric.html.

Since the maximum amount of kda someone can have in their posssession is around
1 billion and also the maximum precision of any number represented by pact needs
12 digits past the decimal point, we have chosen the numbers 21 and 12 for the
precision and scale of the newtype KDAScientific.

-}

newtype KDADecimal = KDADecimal { getKDADecimal :: Decimal }
  deriving Eq

instance Show KDADecimal where
  show (KDADecimal d) = show d

instance BA.HasColumnType KDADecimal where
  -- defaultColumnType _ = SqlStdType $ numericType (Just (21, Just 12))
  defaultColumnType _ = SqlStdType $ numericType Nothing
  defaultTypeCast _ = Just "numeric"

instance HasSqlValueSyntax PgValueSyntax KDADecimal where
  sqlValueSyntax = defaultPgValueSyntax

instance ToField KDADecimal where
  toField (KDADecimal s) = toField s

instance ToField Decimal where
  toField = undefined

instance FromField Decimal where
  fromField = undefined

instance FromBackendRow Postgres KDADecimal where

instance FromField KDADecimal where
  fromField f mb = fmap KDADecimal $ fromField f mb
