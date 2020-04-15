{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Chainweb.Lookups
  ( -- * Types
    Low(..)
  , High(..)
    -- * Endpoints
  , headersBetween
  , payloadWithOutputs
  , allChains
    -- * Transformations
  , mkBlockTransactions
  , bpwoMinerKeys
  ) where

import           BasePrelude
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.PactCommand
import           Chainweb.Api.Payload
import qualified Chainweb.Api.Transaction as CW
import           Chainweb.Env
import           ChainwebData.Types ()
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction
import           Control.Error.Util (hush)
import           Data.Aeson (Value(..), decode')
import qualified Data.ByteString.Base64.URL as B64
import qualified Data.List.NonEmpty as NEL
import           Data.Serialize.Get (runGet)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Lens.Micro (to, (^..), _Just)
import           Lens.Micro.Aeson
import           Network.HTTP.Client hiding (Proxy)

--------------------------------------------------------------------------------
-- Types

newtype Low = Low Int deriving newtype (Show)

newtype High = High Int deriving newtype (Eq, Ord, Show)

--------------------------------------------------------------------------------
-- Endpoints

headersBetween :: Env -> (ChainId, Low, High) -> IO [BlockHeader]
headersBetween (Env m _ u (ChainwebVersion v) _) (cid, Low low, High up) = do
  req <- parseRequest url
  res <- httpLbs (req { requestHeaders = requestHeaders req <> encoding }) m
  pure . (^.. key "items" . values . _String . to f . _Just) $ responseBody res
  where
    url = "https://" <> urlToString u <> query
    query = printf "/chainweb/0.0/%s/chain/%d/header?minheight=%d&maxheight=%d"
      (T.unpack v) (unChainId cid) low up
    encoding = [("accept", "application/json")]

    f :: T.Text -> Maybe BlockHeader
    f = hush . (B64.decode . T.encodeUtf8 >=> runGet decodeBlockHeader)

payloadWithOutputs :: Env -> T2 ChainId DbHash -> IO (Maybe BlockPayloadWithOutputs)
payloadWithOutputs (Env m _ u (ChainwebVersion v) _) (T2 cid0 hsh0) = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://" <> urlToString u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh <> "/outputs"
    cid = T.pack $ show cid0
    hsh = unDbHash hsh0

-- | Query a node for the `ChainId` values its current `ChainwebVersion` has
-- available.
allChains :: Manager -> Url -> IO (Maybe (NonEmpty ChainId))
allChains m u = do
  req <- parseRequest $ "https://" <> urlToString u <> "/info"
  res <- httpLbs req m
  pure . NEL.nonEmpty $ responseBody res ^.. lns
  where
    lns = key "nodeChains" . values . _JSON . to readMaybe . _Just . to ChainId

--------------------------------------------------------------------------------
-- Transformations

-- | Derive useful database entries from a `Block` and its payload.
mkBlockTransactions :: Block -> BlockPayloadWithOutputs -> [Transaction]
mkBlockTransactions b pl = map (mkTransaction b) $ _blockPayloadWithOutputs_transactionsWithOutputs pl

bpwoMinerKeys :: BlockPayloadWithOutputs -> [T.Text]
bpwoMinerKeys = _minerData_publicKeys . _blockPayloadWithOutputs_minerData

mkTransaction :: Block -> (CW.Transaction, TransactionOutput) -> Transaction
mkTransaction b (tx,txo) = Transaction
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
  , _tx_proof = _cont_proof <$> cnt

  , _tx_gas = _toutGas txo
  , _tx_badResult = badres
  , _tx_goodResult = goodres
  , _tx_logs = hashB64U <$> _toutLogs txo
  , _tx_metadata = PgJSONB <$> _toutMetaData txo
  , _tx_continuation = PgJSONB <$> _toutContinuation txo
  , _tx_txid = _toutTxId txo
  }
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
    (badres, goodres) = case _toutResult txo of
      PactResult (Left v) -> (Just $ PgJSONB v, Nothing)
      PactResult (Right v) -> (Nothing, Just $ PgJSONB v)
