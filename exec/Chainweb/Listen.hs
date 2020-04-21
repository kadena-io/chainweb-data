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
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import qualified Data.ByteString.Char8 as B
import qualified Data.Pool as P
import           Data.Tuple.Strict (T2(..))
import           Database.Beam.Postgres (Connection)
import           Network.HTTP.Client
import           Network.Wai.EventSource.Streaming
import qualified Streaming.Prelude as SP
import           System.IO (hFlush, stdout)

---

listen :: Env -> IO ()
listen e = listenWithHandler e (getOutputsAndInsert e)

listenWithHandler :: Env -> (PowHeader -> IO a) -> IO ()
listenWithHandler (Env mgr _ u _ _) handler =
  withEvents (req u) mgr $ SP.mapM_ handler . dataOnly @PowHeader

getOutputsAndInsert :: Env -> PowHeader -> IO ()
getOutputsAndInsert e ph@(PowHeader h _) = do
  let !pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
  payloadWithOutputs e pair >>= \case
    Nothing -> printf "[FAIL] Couldn't fetch parent for: %s\n"
      (hashB64U $ _blockHeader_hash h)
    Just pl -> do
      insertNewHeader (_env_dbConnPool e) ph pl
      printf "%d" (unChainId $ _blockHeader_chainId h) >> hFlush stdout

insertNewHeader :: P.Pool Connection -> PowHeader -> BlockPayloadWithOutputs -> IO ()
insertNewHeader pool ph pl = do
  let !m = _blockPayloadWithOutputs_minerData pl
      !b = asBlock ph m
      !t = mkBlockTransactions b pl
      !k = bpwoMinerKeys pl
  writes pool b k t

req :: Url -> Request
req (Url h p) = defaultRequest
  { host = B.pack h
  , path = "chainweb/0.0/mainnet01/header/updates"  -- TODO Parameterize as needed.
  , port = p
  , secure = True
  , method = "GET"
  , requestBody = mempty
  , responseTimeout = responseTimeoutNone
  , checkResponse = throwErrorStatusCodes }
