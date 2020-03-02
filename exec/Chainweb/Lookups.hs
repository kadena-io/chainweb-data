{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Lookups
  ( -- * Types
    Low(..)
  , High(..)
    -- * Endpoints
  , headersBetween
  , payload
    -- * Transformations
  , txs
  , miner
  , keys
  ) where

import           BasePrelude
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.PactCommand
import           Chainweb.Api.Payload
import qualified Chainweb.Api.Transaction as CW
import           Chainweb.Env
import           Chainweb.Types ()
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction
import           Data.Aeson (Value(..), decode')
import qualified Data.Text as T
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Lens.Micro ((^..))
import           Lens.Micro.Aeson (key, values, _JSON)
import           Network.HTTP.Client hiding (Proxy)

--------------------------------------------------------------------------------
-- Types

newtype Low = Low Int deriving newtype (Show)

newtype High = High Int deriving newtype (Eq, Ord, Show)

--------------------------------------------------------------------------------
-- Endpoints

headersBetween :: Env -> (ChainId, Low, High) -> IO [BlockHeader]
headersBetween (Env m _ (Url u) (ChainwebVersion v)) (cid, Low low, High up) = do
  req <- parseRequest url
  res <- httpLbs (req { requestHeaders = requestHeaders req <> encoding }) m
  pure . (^.. key "items" . values . _JSON) $ responseBody res
  where
    url = "https://" <> u <> query
    query = printf "/chainweb/0.0/%s/chain/%d/header?minheight=%d&maxheight=%d"
      (T.unpack v) (unChainId cid) low up
    encoding = [("accept", "application/json;blockheader-encoding=object")]

payload :: Env -> T2 ChainId DbHash -> IO (Maybe BlockPayload)
payload (Env m _ (Url u) (ChainwebVersion v)) (T2 cid0 hsh0) = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://" <> u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh
    cid = T.pack $ show cid0
    hsh = unDbHash hsh0

--------------------------------------------------------------------------------
-- Transformations

-- | Derive useful database entries from a `Block` and its payload.
txs :: Block -> BlockPayload -> [Transaction]
txs b pl = map (transaction b) $ _blockPayload_transactions pl

miner :: BlockPayload -> MinerData
miner = _blockPayload_minerData

keys :: BlockPayload -> [T.Text]
keys = _minerData_publicKeys . _blockPayload_minerData

transaction :: Block -> CW.Transaction -> Transaction
transaction b tx = Transaction
  { _tx_chainId = _block_chainId b
  , _tx_block = pk b
  , _tx_creationTime = posixSecondsToUTCTime $ _chainwebMeta_creationTime mta
  , _tx_ttl = _chainwebMeta_ttl mta
  , _tx_gasLimit = _chainwebMeta_gasLimit mta
  , _tx_gasPrice = _chainwebMeta_gasPrice mta
  , _tx_sender = _chainwebMeta_sender mta
  , _tx_nonce = _pactCommand_nonce cmd
  , _tx_requestKey = hashB64U $ CW._transaction_hash tx
  , _tx_code = _exec_code <$> exc
  , _tx_pactId = _cont_pactId <$> cnt
  , _tx_rollback = _cont_rollback <$> cnt
  , _tx_step = _cont_step <$> cnt
  , _tx_data = (PgJSONB . Object . _cont_data <$> cnt)
    <|> (PgJSONB . Object <$> (exc >>= _exec_data))
  , _tx_proof = _cont_proof <$> cnt }
  where
    cmd = CW._transaction_cmd tx
    mta = _pactCommand_meta cmd
    pay = _pactCommand_payload cmd
    exc = case pay of
      ExecPayload e -> Just e
      ContPayload _ -> Nothing
    cnt = case pay of
      ExecPayload _ -> Nothing
      ContPayload c -> Just c
