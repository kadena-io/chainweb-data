{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ChainwebData.TxSummary where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Text (Text)
import Data.Time
import Database.Beam
------------------------------------------------------------------------------
import ChainwebDb.Types.DbHash
------------------------------------------------------------------------------

data TxResult
  = TxSucceeded
  | TxFailed
  | TxUnexpected -- Shouldn't happen...here for totality
  deriving (Eq,Ord,Show,Read,Enum,Generic)

instance ToJSON TxResult where
    toEncoding = genericToEncoding defaultOptions
instance FromJSON TxResult

data TxSummary = TxSummary
  { _txSummary_chain :: Int
  , _txSummary_height :: Int
  , _txSummary_blockHash :: DbHash
  , _txSummary_creationTime :: UTCTime
  , _txSummary_requestKey :: Text
  , _txSummary_sender :: Text
  , _txSummary_code :: Maybe Text
  , _txSummary_result :: TxResult
  } deriving (Eq,Ord,Show,Generic)

instance ToJSON TxSummary where
    toJSON s = object
      [ "chain" .= _txSummary_chain s
      , "height" .= _txSummary_height s
      , "blockHash" .= _txSummary_blockHash s
      , "creationTime" .= _txSummary_creationTime s
      , "requestKey" .= _txSummary_requestKey s
      , "sender" .= _txSummary_sender s
      , "code" .= _txSummary_code s
      , "result" .= _txSummary_result s
      ]

instance FromJSON TxSummary where
    parseJSON = withObject "TxSummary" $ \v -> TxSummary
      <$> v .: "chain"
      <*> v .: "height"
      <*> v .: "blockHash"
      <*> v .: "creationTime"
      <*> v .: "requestKey"
      <*> v .: "sender"
      <*> v .: "code"
      <*> v .: "result"
