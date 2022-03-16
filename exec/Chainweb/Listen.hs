{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.Listen
  ( listen
  , listenWithHandler
  , getOutputsAndInsert
  , insertNewHeader
  ) where

import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (unChainId)
import           Chainweb.Api.Hash
import           Chainweb.Api.NodeInfo
import           ChainwebData.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import           ChainwebDb.Types.DbHash (DbHash(..))
import           Control.Exception
import qualified Data.ByteString.Char8 as B
import qualified Data.Pool as P
import           Data.String
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Tuple.Strict (T2(..))
import           Database.Beam.Postgres (Connection)
import           Network.HTTP.Client
import           Network.Wai.EventSource.Streaming
import qualified Streaming.Prelude as SP
import           System.IO (hFlush, stdout)
import           System.Logger.Types hiding (logg)
import           Text.Printf

---

listen :: Env -> IO ()
listen e = listenWithHandler e (getOutputsAndInsert e)

-- | This function does not do any exception handling because it is used in
-- multiple contexts.  When running via the 'listen' command we want it to fail
-- fast so the failure will be visible to the calling context and appropriate
-- action can be taken.  But when running via the 'server' command we want the
-- server to be tolerant of failures.  To accomplish both of these goals we
-- fail fast here and let the server logic be ressponsible for handling
-- failures and retrying appropriately.
listenWithHandler :: Env -> (PowHeader -> IO a) -> IO ()
listenWithHandler env handler = handle onError $
    withEvents (mkRequest us cv) mgr $ SP.mapM_ handler . dataOnly @PowHeader
  where
    mgr = _env_httpManager env
    us = _env_serviceUrlScheme env
    cv = ChainwebVersion $ _nodeInfo_chainwebVer $ _env_nodeInfo env
    onError :: SomeException -> IO ()
    onError e = _env_logger env Error $ "listenWithHandler caught exception: " <> T.pack (show e)

getOutputsAndInsert :: Env -> PowHeader -> IO ()
getOutputsAndInsert env ph@(PowHeader h _) = do
  let !pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
      logg = _env_logger env
  payloadWithOutputs env pair >>= \case
    Left e -> do
      logg Error $ fromString $ printf "Couldn't fetch parent for: %s"
             (hashB64U $ _blockHeader_hash h)
      logg Info $ fromString $ show e
    Right pl -> do
      insertNewHeader (_env_dbConnPool env) ph pl
      logg Info (fromString $ printf "%d" (unChainId $ _blockHeader_chainId h)) >> hFlush stdout

insertNewHeader :: P.Pool Connection -> PowHeader -> BlockPayloadWithOutputs -> IO ()
insertNewHeader pool ph pl = do
  let !m = _blockPayloadWithOutputs_minerData pl
      !b = asBlock ph m
      !t = mkBlockTransactions b pl
      !es = mkBlockEvents (fromIntegral $ _blockHeader_height $ _hwp_header ph) (_blockHeader_chainId $ _hwp_header ph) (DbHash $ hashB64U $ _blockHeader_hash $ _hwp_header ph) pl
      !ss = concat $ map (mkTransactionSigners . fst) (_blockPayloadWithOutputs_transactionsWithOutputs pl)
      !tf = mkTransferRows (fromIntegral $ _blockHeader_height $ _hwp_header ph) (_blockHeader_chainId $ _hwp_header ph) (DbHash $ hashB64U $ _blockHeader_hash $ _hwp_header ph) pl undefined
      !k = bpwoMinerKeys pl
  writes pool b k t es ss tf

mkRequest :: UrlScheme -> ChainwebVersion -> Request
mkRequest us (ChainwebVersion cv) = defaultRequest
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
