{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module ChainwebDb.Types.Event where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text, unpack)
import Database.Beam
import Database.Beam.Backend.SQL
import Database.Beam.Backend.Types
import Database.Beam.Migrate
import Database.Beam.Postgres (PgJSONB)
import Text.Read
import Text.Read.Lex (Lexeme(Ident))
------------------------------------------------------------------------------

data SourceType = Source_Coinbase | Source_Tx deriving (Eq, Ord, Generic, Enum, Bounded)

instance Show SourceType where
  show c = case c of
    Source_Coinbase -> "coinbase"
    Source_Tx -> "tx"

instance Read SourceType where
  readPrec =
    parens $
      do Ident s <- lexP
         case s of
           "coinbase" -> return Source_Coinbase
           "tx" -> return Source_Tx
           _ -> pfail
  readListPrec = readListPrecDefault
  readList = readListDefault

instance ToJSON SourceType where
  toEncoding = genericToEncoding defaultOptions

instance FromJSON SourceType

instance HasSqlValueSyntax be String => HasSqlValueSyntax be SourceType where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend be => HasSqlEqualityCheck be SourceType

instance (BeamBackend be, FromBackendRow be Text) => FromBackendRow be SourceType where
  fromBackendRow = read . unpack <$> fromBackendRow

instance BeamMigrateSqlBackend be => HasDefaultSqlDataType be SourceType where
  defaultSqlDataType _ _ _ = varCharType Nothing Nothing

------------------------------------------------------------------------------
data EventT f = Event
  { _ev_sourceKey :: C f Text
  , _ev_idx :: C f Int64
  , _ev_sourceType :: C f SourceType
  , _ev_qualName :: C f Text
  , _ev_name :: C f Text
  , _ev_module :: C f Text
  , _ev_moduleHash :: C f Text
  , _ev_paramText :: C f Text
  , _ev_params :: C f (PgJSONB [Value])
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Event
  (LensFor ev_sourceKey)
  (LensFor ev_idx)
  (LensFor ev_sourceType)
  (LensFor ev_name)
  (LensFor ev_qualName)
  (LensFor ev_module)
  (LensFor ev_moduleHash)
  (LensFor ev_paramText)
  (LensFor ev_params)
  = tableLenses

type Event = EventT Identity
type EventId = PrimaryKey EventT Identity

instance Table EventT where
  data PrimaryKey EventT f = EventId (C f Text) (C f Int64)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey t = EventId (_ev_sourceKey t) (_ev_idx t)


