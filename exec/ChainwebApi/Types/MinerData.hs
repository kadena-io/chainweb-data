{-# LANGUAGE OverloadedStrings #-}

module ChainwebApi.Types.MinerData where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
------------------------------------------------------------------------------

data MinerData = MinerData
  { _minerData_account    :: Text
  , _minerData_predicate  :: Text
  , _minerData_publicKeys :: [Text]
  } deriving (Eq,Ord,Show)

instance FromJSON MinerData where
  parseJSON = withObject "MinerData" $ \o -> MinerData
    <$> o .: "account"
    <*> o .: "predicate"
    <*> o .: "public-keys"
