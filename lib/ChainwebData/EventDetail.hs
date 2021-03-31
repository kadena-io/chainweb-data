{-# LANGUAGE DeriveGeneric #-}

module ChainwebData.EventDetail where

import ChainwebData.Util
import Data.Aeson
import Data.Text (Text)
import Data.Time
import GHC.Generics

data EventDetail = EventDetail
  { _evDetail_name :: Text
  , _evDetail_params :: [Value]
  , _evDetail_moduleHash :: Text
  , _evDetail_chain :: Int
  , _evDetail_height :: Int
  , _evDetail_blockTime :: UTCTime
  , _evDetail_blockHash :: Text
  , _evDetail_requestKey :: Text
  , _evDetail_idx :: Int
  } deriving (Eq,Show,Generic)

instance ToJSON EventDetail where
    toJSON = lensyToJSON 10

instance FromJSON EventDetail where
    parseJSON = lensyParseJSON 10
