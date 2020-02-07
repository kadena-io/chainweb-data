{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Chainweb.Types
  ( PowHeader(..)
  , asHeader
  , asPow
  ) where

import           BasePrelude
import           Chainweb.Api.BlockHeader (BlockHeader(..), powHash)
import           Chainweb.Api.BytesLE
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           ChainwebDb.Types.DbHash (DbHash(..))
import           ChainwebDb.Types.Header
import           Data.Aeson
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import           Lens.Micro ((^?))
import           Lens.Micro.Aeson (key, _JSON)
import           Network.Wai.EventSource.Streaming

---

data PowHeader = PowHeader
  { _hwp_header :: BlockHeader
  , _hwp_powHash :: T.Text }

instance FromEvent PowHeader where
  fromEvent bs = do
    hu <- decode' @Value $ BL.fromStrict bs
    PowHeader
      <$> (hu ^? key "header"  . _JSON)
      <*> (hu ^? key "powHash" . _JSON)

asPow :: BlockHeader -> PowHeader
asPow bh = PowHeader bh (hashB64U $ powHash bh)

asHeader :: PowHeader -> Header
asHeader (PowHeader bh ph) = Header
  { _header_creationTime = floor $ _blockHeader_creationTime bh
  , _header_chainId      = unChainId $ _blockHeader_chainId bh
  , _header_height       = _blockHeader_height bh
  , _header_parent       = DbHash . hashB64U $ _blockHeader_parent bh
  , _header_hash         = DbHash . hashB64U $ _blockHeader_hash bh
  , _header_payloadHash  = DbHash . hashB64U $ _blockHeader_payloadHash bh
  , _header_target       = DbHash . hexBytesLE $ _blockHeader_target bh
  , _header_weight       = DbHash . hexBytesLE $ _blockHeader_weight bh
  , _header_epochStart   = floor $ _blockHeader_epochStart bh
  , _header_nonce        = _blockHeader_nonce bh
  , _header_powHash      = DbHash ph }

--------------------------------------------------------------------------------
-- Orphans

instance ToJSON BlockHeader where
  toJSON _ = object []
