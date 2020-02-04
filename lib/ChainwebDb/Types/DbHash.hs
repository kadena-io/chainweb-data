{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text)
import Database.Beam.Backend.SQL.Row (FromBackendRow)
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax)
import Database.Beam.Migrate (HasDefaultSqlDataType)
import Database.Beam.Query (HasSqlEqualityCheck)
import Database.Beam.Sqlite (Sqlite)
import Database.Beam.Sqlite.Syntax (SqliteValueSyntax)
------------------------------------------------------------------------------

-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash = DbHash { unDbHash :: Text }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
  deriving newtype (HasSqlValueSyntax SqliteValueSyntax, HasDefaultSqlDataType Sqlite)
  deriving newtype (FromBackendRow Sqlite, HasSqlEqualityCheck Sqlite)
