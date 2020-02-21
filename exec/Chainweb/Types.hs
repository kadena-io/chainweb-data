{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Chainweb.Types
  ( PowHeader(..)
  , asBlock
  , asPow
  , hash
  ) where

import           BasePrelude
import           Chainweb.Api.BlockHeader (BlockHeader(..), powHash)
import           Chainweb.Api.BytesLE
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash (DbHash(..))
import           ChainwebDb.Types.Miner
import           Data.Aeson
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import           Database.Beam.Schema (pk)
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

asBlock :: PowHeader -> Miner -> Block
asBlock (PowHeader bh ph) m = Block
  { _block_creationTime = floor $ _blockHeader_creationTime bh
  , _block_chainId      = unChainId $ _blockHeader_chainId bh
  , _block_height       = _blockHeader_height bh
  , _block_parent       = DbHash . hashB64U $ _blockHeader_parent bh
  , _block_hash         = DbHash . hashB64U $ _blockHeader_hash bh
  , _block_payload      = DbHash . hashB64U $ _blockHeader_payloadHash bh
  , _block_target       = DbHash . hexBytesLE $ _blockHeader_target bh
  , _block_weight       = DbHash . hexBytesLE $ _blockHeader_weight bh
  , _block_epochStart   = floor $ _blockHeader_epochStart bh
  , _block_nonce        = _blockHeader_nonce bh
  , _block_powHash      = DbHash ph
  , _block_miner        = pk m }

-- | Convert to the "pretty" hash representation that URLs, etc., expect.
hash :: Hash -> DbHash
hash = DbHash . hashB64U

--------------------------------------------------------------------------------
-- Orphans

instance ToJSON BlockHeader where
  toJSON _ = object []
