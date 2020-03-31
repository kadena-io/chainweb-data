{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Database.Beam.Backend.SQL.Row (FromBackendRow)
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax)
import Database.Beam.Migrate (HasDefaultSqlDataType)
import Database.Beam.Postgres (Postgres)
import Database.Beam.Postgres.Syntax (PgValueSyntax)
import Database.Beam.Query (HasSqlEqualityCheck)
import GHC.Generics
------------------------------------------------------------------------------

-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash = DbHash { unDbHash :: Text }
  deriving stock (Eq, Ord, Show, Generic)
  deriving newtype (HasSqlValueSyntax PgValueSyntax, HasDefaultSqlDataType Postgres)
  deriving newtype (FromBackendRow Postgres, HasSqlEqualityCheck Postgres)

instance ToJSON DbHash where
    toJSON = toJSON . unDbHash

instance FromJSON DbHash where
    parseJSON = withText "DbHash" $ \v -> pure $ DbHash v
