{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax)
import Database.Beam.Migrate (HasDefaultSqlDataType)
import Database.Beam.Sqlite (Sqlite)
import Database.Beam.Sqlite.Syntax (SqliteValueSyntax)
import GHC.Generics (Generic)
import Servant.API (ToHttpApiData(..))
------------------------------------------------------------------------------

-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash = DbHash { unDbHash :: Text }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
  deriving newtype (HasSqlValueSyntax SqliteValueSyntax, HasDefaultSqlDataType Sqlite)
  deriving newtype (ToHttpApiData)
