module ChainwebApi.Types.BytesLE where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import           Data.Text (Text)
import qualified Data.Text.Encoding as T
------------------------------------------------------------------------------
import           ChainwebApi.Types.Base64Url
------------------------------------------------------------------------------

newtype BytesLE = BytesLE
  { unBytesLE :: ByteString
  } deriving (Eq,Ord,Show)

hexBytesLE :: BytesLE -> Text
hexBytesLE = T.decodeUtf8 . B16.encode . unBytesLE

leToInteger :: ByteString -> Integer
leToInteger bs = B.foldl' (\a b -> a * 256 + fromIntegral b) 0 bs

instance FromJSON BytesLE where
  parseJSON = withText "BytesLE" $
    either fail (return . BytesLE . B.reverse) . decodeB64UrlNoPaddingText
