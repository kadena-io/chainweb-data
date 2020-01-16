{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Database.Beam
------------------------------------------------------------------------------

-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash = DbHash { unDbHash :: Text }
  deriving stock (Eq,Ord,Show,Generic)
  deriving anyclass (ToJSON, FromJSON)
