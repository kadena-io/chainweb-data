{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- TODO Remove once orphan is adopted.

module Main ( main ) where

import           ChainwebApi.Types.BlockHeader (BlockHeader)
import           Data.Aeson (decode')
import qualified Data.ByteString.Lazy as BL
import           Data.Function ((&))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (tlsManagerSettings)
import           Network.Wai.EventSource.Streaming
import           Servant.Client.Core (BaseUrl(..), Scheme(..))
import qualified Streaming.Prelude as SP

---

main :: IO ()
main = do
  m <- newManager tlsManagerSettings
  headerStream m $ BaseUrl Https "honmono.fosskers.ca" 443 ""

headerStream :: Manager -> BaseUrl -> IO ()
headerStream m u = withEvents (req u) m $ \updates -> updates
  & dataOnly @BlockHeader
  & SP.print

req :: BaseUrl -> Request
req u = defaultRequest
  { host = T.encodeUtf8 . T.pack . baseUrlHost $ u
  , path = "chainweb/0.0/mainnet01/header/updates"  -- TODO Parameterize as needed.
  , port = baseUrlPort u
  , secure = True
  , method = "GET"
  , requestBody = mempty
  , responseTimeout = responseTimeoutNone
  , checkResponse = throwErrorStatusCodes }

-- TODO Adopt this orphan.
instance FromEvent BlockHeader where
  fromEvent = decode' . BL.fromStrict
