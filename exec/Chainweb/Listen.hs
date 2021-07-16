{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.Listen
  ( listen
  , listenWithHandler
  , getOutputsAndInsert
  , insertNewHeader
  ) where

import           BasePrelude hiding (insert)
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (unChainId)
import           Chainweb.Api.Hash
import           Chainweb.Api.NodeInfo
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import           ChainwebDb.Types.DbHash (DbHash(..))
import qualified Data.ByteString.Char8 as B
import qualified Data.Pool as P
import           Data.Text.Encoding
import           Data.Tuple.Strict (T2(..))
import           Database.Beam.Postgres (Connection)
import           Network.HTTP.Client
import           Network.Wai.EventSource.Streaming
import qualified Streaming.Prelude as SP
import           System.IO (hFlush, stdout)
import           System.Logger.Types hiding (logg)

---

listen :: Env -> IO ()
listen e = listenWithHandler e (getOutputsAndInsert e)

listenWithHandler :: Env -> (PowHeader -> IO a) -> IO ()
listenWithHandler env handler =
  withEvents (req us cv) mgr $ SP.mapM_ handler . dataOnly @PowHeader
  where
    mgr = _env_httpManager env
    us = _env_serviceUrlScheme env
    cv = ChainwebVersion $ _nodeInfo_chainwebVer $ _env_nodeInfo env

getOutputsAndInsert :: Env -> PowHeader -> IO ()
getOutputsAndInsert e ph@(PowHeader h _) = do
  let !pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
      logg = _env_logger e
  payloadWithOutputs e pair >>= \case
    Nothing -> logg Error $ fromString $ printf "[FAIL] Couldn't fetch parent for: %s\n"
      (hashB64U $ _blockHeader_hash h)
    Just pl -> do
      insertNewHeader (_env_dbConnPool e) ph pl
      logg Info (fromString $ printf "%d" (unChainId $ _blockHeader_chainId h)) >> hFlush stdout

insertNewHeader :: P.Pool Connection -> PowHeader -> BlockPayloadWithOutputs -> IO ()
insertNewHeader pool ph pl = do
  let !m = _blockPayloadWithOutputs_minerData pl
      !b = asBlock ph m
      !t = mkBlockTransactions b pl
      !es = mkBlockEvents (fromIntegral $ _blockHeader_height $ _hwp_header ph) (_blockHeader_chainId $ _hwp_header ph) (DbHash $ hashB64U $ _blockHeader_parent $ _hwp_header ph) pl
      !k = bpwoMinerKeys pl
  writes pool b k t es

req :: UrlScheme -> ChainwebVersion -> Request
req us (ChainwebVersion cv) = defaultRequest
  { host = B.pack $ urlHost u
  , path = "chainweb/0.0/" <> encodeUtf8 cv <> "/header/updates"  -- TODO Parameterize as needed.
  , port = urlPort u
  , secure = secureFlag
  , method = "GET"
  , requestBody = mempty
  , responseTimeout = responseTimeoutNone
  , checkResponse = throwErrorStatusCodes }
  where
    u = usUrl us
    secureFlag = case usScheme us of
                   Http -> False
                   Https -> True
