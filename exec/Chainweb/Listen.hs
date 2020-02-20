{-# LANGUAGE TypeApplications #-}


module Chainweb.Listen ( listen ) where

import           BasePrelude hiding (insert)
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Types
import qualified Data.ByteString.Char8 as B
import           Database.Beam
import           Database.Beam.Postgres (Connection, runBeamPostgres)
import           Network.HTTP.Client hiding (withConnection)
import           Network.Wai.EventSource.Streaming
import qualified Streaming.Prelude as SP

---

listen :: Env -> IO ()
listen (Env m c u _) = withConnection c $ \conn ->
  withEvents (req u) m
    $ SP.mapM_ (\bh -> f conn bh >> h bh)
    . dataOnly @PowHeader
  where
    f :: Connection -> PowHeader -> IO ()
    f conn bh = runBeamPostgres conn
      . runInsert
      . insert (blocks database)
      $ insertValues [asHeader bh]

    h :: PowHeader -> IO ()
    h (PowHeader bh _) = printf "[OKAY] Chain %d: %d: %s\n"
      (unChainId $ _blockHeader_chainId bh)
      (_blockHeader_height bh)
      (hashB64U $ _blockHeader_hash bh)

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
