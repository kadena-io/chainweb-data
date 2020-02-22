{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.Listen ( listen ) where

import           BasePrelude hiding (insert)
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.Hash
import           Chainweb.Env
import           Chainweb.Types
import           Chainweb.Worker
import           ChainwebDb.Types.Block
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
listen e@(Env mgr c u _) = withPool c $ \pool ->
  withEvents (req u) mgr $ SP.mapM_ (f pool) . dataOnly @PowHeader
  where
    f :: P.Pool Connection -> PowHeader -> IO ()
    f pool ph@(PowHeader h _) = do
      let !pair = T2 (_blockHeader_chainId h) (hash $ _blockHeader_payloadHash h)
      payload e pair >>= \case
        Nothing -> printf "[FAIL] Couldn't fetch parent for: %s\n"
          (hashB64U $ _blockHeader_hash h)
        Just pl -> do
          let !m = miner pl
              !b = asBlock ph m
              !t = txs b pl
          writes pool b $ T2 m t
          printf "%d" (_block_chainId b) >> hFlush stdout

req :: Url -> Request
req (Url u) = defaultRequest
  { host = B.pack u
  , path = "chainweb/0.0/mainnet01/header/updates"  -- TODO Parameterize as needed.
  , port = 443
  , secure = True
  , method = "GET"
  , requestBody = mempty
  , responseTimeout = responseTimeoutNone
  , checkResponse = throwErrorStatusCodes }
