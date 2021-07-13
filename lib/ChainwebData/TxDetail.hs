{-# LANGUAGE DeriveGeneric #-}

module ChainwebData.TxDetail where

import ChainwebData.Util
import Data.Aeson
import Data.Text (Text)
import Data.Time
import GHC.Generics

data TxEvent = TxEvent
  { _txEvent_name :: Text
  , _txEvent_params :: [Value]
  } deriving (Eq,Show,Generic)

instance ToJSON TxEvent where toJSON = lensyToJSON 9
instance FromJSON TxEvent where parseJSON = lensyParseJSON 9


data TxDetail = TxDetail
  { _txDetail_ttl ::  Int
  , _txDetail_gasLimit :: Int
  , _txDetail_gasPrice :: Double
  , _txDetail_nonce :: Text
  , _txDetail_pactId :: Maybe Text
  , _txDetail_rollback :: Maybe Bool
  , _txDetail_step :: Maybe Int
  , _txDetail_data :: Value
  , _txDetail_proof :: (Maybe Text)
  , _txDetail_gas :: Int
  , _txDetail_result :: Value
  , _txDetail_logs :: Text
  , _txDetail_metadata :: Value
  , _txDetail_continuation :: Maybe Value
  , _txDetail_txid :: Int
  , _txDetail_chain :: Int
  , _txDetail_height :: Int
  , _txDetail_blockTime :: UTCTime
  , _txDetail_blockHash :: Text
  , _txDetail_creationTime :: UTCTime
  , _txDetail_requestKey :: Text
  , _txDetail_sender :: Text
  , _txDetail_code :: Maybe Text
  , _txDetail_success :: Bool
  , _txDetail_events :: [TxEvent]
  } deriving (Eq,Show,Generic)

instance ToJSON TxDetail where
    toJSON = lensyToJSON 10

instance FromJSON TxDetail where
    parseJSON = lensyParseJSON 10
