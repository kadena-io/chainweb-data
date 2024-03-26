{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Database.Beam.Backend.SQL hiding (tableName)
import Database.Beam.Backend.SQL.Row ()
import Database.Beam.Backend.SQL.SQL92 ()
import Database.Beam.Postgres (Postgres)
import Database.Beam.Postgres.Syntax (PgValueSyntax)
import Database.Beam.Query (HasSqlEqualityCheck)
import GHC.Generics
------------------------------------------------------------------------------


data PayloadHash = PayloadHash
data PowHash = PowHash
data BlockHash = BlockHash
data TxHash = TxHash -- i.e. requestkey

-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash t = DbHash { unDbHash :: Text }
  deriving stock (Eq, Ord, Show, Generic)
  deriving newtype (HasSqlValueSyntax PgValueSyntax)
  deriving newtype (FromBackendRow Postgres, HasSqlEqualityCheck Postgres)

instance ToJSON (DbHash t) where
    toJSON = toJSON . unDbHash

instance FromJSON (DbHash t) where
    parseJSON = withText "DbHash" $ \v -> pure $ DbHash v
