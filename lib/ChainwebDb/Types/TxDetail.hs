{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ChainwebData.TxDetail where

------------------------------------------------------------------------------
import           Data.Aeson
import Data.Aeson.Types
import Data.Char
import           Data.Text (Text)
import           Data.Time
import           GHC.Generics
------------------------------------------------------------------------------


data TxResult
  = TxSucceeded
  | TxFailed
  | TxUnexpected -- Shouldn't happen...here for totality
  deriving (Eq,Ord,Show,Read,Enum,Generic)

instance ToJSON TxResult where
    toEncoding = genericToEncoding defaultOptions
instance FromJSON TxResult

data TxDetail = TxDetail
  {
    _txDetail_ttl ::  Int
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
  } deriving (Eq,Show,Generic)

instance ToJSON TxDetail where
    toJSON = lensyToJSON 10

instance FromJSON TxDetail where
    parseJSON = lensyParseJSON 10

lensyToJSON
  :: (Generic a, GToJSON Zero (Rep a)) => Int -> a -> Value
lensyToJSON n = genericToJSON (lensyOptions n)

lensyParseJSON
  :: (Generic a, GFromJSON Zero (Rep a)) => Int -> Value -> Parser a
lensyParseJSON n = genericParseJSON (lensyOptions n)

lensyOptions :: Int -> Options
lensyOptions n = defaultOptions { fieldLabelModifier = lensyConstructorToNiceJson n }

lensyConstructorToNiceJson :: Int -> String -> String
lensyConstructorToNiceJson n fieldName = firstToLower $ drop n fieldName
  where
    firstToLower (c:cs) = toLower c : cs
    firstToLower _ = error $ "lensyConstructorToNiceJson: bad arguments: " ++ show (n,fieldName)
