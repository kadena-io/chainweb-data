{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Chainweb.Server ( server ) where

import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.BytesLE
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           Chainweb.Database
import           Chainweb.Env
import           ChainwebDb.Types.DbHash (DbHash(..))
import           ChainwebDb.Types.Header
import           Data.Aeson (ToJSON(..), Value, decode', object)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Database.Beam
import           Database.Beam.Sqlite (Sqlite, runBeamSqlite)
import           Database.SQLite.Simple (Connection)
import           Lens.Micro ((^?))
import           Lens.Micro.Aeson (key, _JSON)
import           Network.HTTP.Client
import           Network.Wai.EventSource.Streaming
import           Servant.Client.Core (BaseUrl(..), Scheme(..))
import qualified Streaming.Prelude as SP
import           Text.Printf (printf)

---

data HeaderWithPow = HeaderWithPow
  { _hwp_header :: BlockHeader
  , _hwp_powHash :: T.Text }

instance FromEvent HeaderWithPow where
  fromEvent bs = do
    hu <- decode' @Value $ BL.fromStrict bs
    HeaderWithPow
      <$> (hu ^? key "header"  . _JSON)
      <*> (hu ^? key "powHash" . _JSON)

server :: Manager -> Connection -> Url -> IO ()
server m conn (Url u) = ingest m (BaseUrl Https u 443 "") conn

ingest :: Manager -> BaseUrl -> Connection -> IO ()
ingest m u c = withEvents (req u) m $ SP.mapM_ (\bh -> f bh >> h bh) . dataOnly @HeaderWithPow
  where
    f :: HeaderWithPow -> IO ()
    f bh = runBeamSqlite c
      . runInsert
      . insert (headers database)
      $ insertExpressions [g bh]

    g :: HeaderWithPow -> HeaderT (QExpr Sqlite s)
    g (HeaderWithPow bh ph) = Header
      { _header_id           = default_
      , _header_creationTime = val_ . floor $ _blockHeader_creationTime bh
      , _header_chainId      = val_ . unChainId $ _blockHeader_chainId bh
      , _header_height       = val_ $ _blockHeader_height bh
      , _header_hash         = val_ . DbHash . hashB64U $ _blockHeader_hash bh
      , _header_payloadHash  = val_ . DbHash . hashB64U $ _blockHeader_payloadHash bh
      , _header_target       = val_ . DbHash . hexBytesLE $ _blockHeader_target bh
      , _header_weight       = val_ . DbHash . hexBytesLE $ _blockHeader_weight bh
      , _header_epochStart   = val_ . floor $ _blockHeader_epochStart bh
      , _header_nonce        = val_ $ _blockHeader_nonce bh
      , _header_powHash      = val_ $ DbHash ph }

    h :: HeaderWithPow -> IO ()
    h (HeaderWithPow bh _) =
      printf "Chain %d: %d\n" (unChainId $ _blockHeader_chainId bh) (_blockHeader_height bh)

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

--------------------------------------------------------------------------------
-- Orphans

instance ToJSON BlockHeader where
  toJSON _ = object []
