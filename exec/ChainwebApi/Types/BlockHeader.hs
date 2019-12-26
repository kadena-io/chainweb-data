{-# LANGUAGE OverloadedStrings #-}

module ChainwebApi.Types.BlockHeader where

------------------------------------------------------------------------------
import Data.Aeson
import Data.Map (Map)
import Data.Readable
import Data.Text (Text)
import Data.Time.Clock.POSIX
import Data.Word
------------------------------------------------------------------------------
import ChainwebApi.Types.BytesLE (BytesLE(..), leToInteger)
import ChainwebApi.Types.Common (BlockHeight)
import ChainwebApi.Types.Hash (Hash)
import Common.Types (ChainId)
------------------------------------------------------------------------------

data BlockHeader = BlockHeader
  { _blockHeader_creationTime :: POSIXTime
  , _blockHeader_parent       :: Hash
  , _blockHeader_height       :: BlockHeight
  , _blockHeader_hash         :: Hash
  , _blockHeader_chainId      :: ChainId
  , _blockHeader_weight       :: BytesLE
  , _blockHeader_epochStart   :: POSIXTime
  , _blockHeader_neighbors    :: Map ChainId Hash
  , _blockHeader_payloadHash  :: Hash
  , _blockHeader_chainwebVer  :: Text
  , _blockHeader_target       :: BytesLE
  , _blockHeader_nonce        :: Word64
  } deriving (Eq,Ord,Show)

blockDifficulty :: BlockHeader -> Double
blockDifficulty =
  fromIntegral . targetToDifficulty . leToInteger . unBytesLE . _blockHeader_target

targetToDifficulty :: Integer -> Integer
targetToDifficulty target = (2 ^ (256 :: Int) - 1) `div` target

instance FromJSON BlockHeader where
  parseJSON = withObject "BlockHeader" $ \o -> BlockHeader
    <$> (fmap (/1000000.0) $ o .: "creationTime")
    <*> o .: "parent"
    <*> o .: "height"
    <*> o .: "hash"
    <*> o .: "chainId"
    <*> o .: "weight"
    <*> (fmap (/1000000.0) $ o .: "epochStart")
    <*> o .: "adjacents"
    <*> (o .: "payloadHash")
    <*> o .: "chainwebVersion"
    <*> o .: "target"
    <*> (fromText =<< (o .: "nonce"))

instance ToJSON BlockHeader where
  toJSON _ = object []
