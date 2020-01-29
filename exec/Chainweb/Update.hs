{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( updates ) where

import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainId
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.PactCommand
import qualified Chainweb.Api.Transaction as CW
import           Chainweb.Database
import           Chainweb.Env (ChainwebVersion, Url(..))
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Header
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction
import           Control.Error.Util (hush)
import           Control.Monad ((>=>))
import           Control.Monad.Trans.Maybe
import           Data.Foldable (traverse_)
import           Data.Proxy (Proxy(..))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Database.Beam
import           Database.Beam.Migrate.Types (unCheckDatabase)
import           Database.Beam.Sqlite (Sqlite, runBeamSqlite)
import           Database.SQLite.Simple (Connection)
import           Network.HTTP.Client (Manager)
import           Servant.API hiding (Header)
import           Servant.Client

---

-- | A wrapper to massage some types below.
data Quad s = Quad
  BlockPayload
  (BlockT (QExpr Sqlite s))
  (MinerT (QExpr Sqlite s))
  [TransactionT (QExpr Sqlite s)]

updates :: Manager -> Connection -> Url -> IO ()
updates m c (Url u) = do
  hs <- runBeamSqlite c . runSelectReturningList $ select prd
  traverse_ (lookups env >=> foobar) hs
  where
    env = ClientEnv m url Nothing
    url = BaseUrl Https u 443 ""
    prd = all_ . headers $ unCheckDatabase dbSettings

foobar :: Maybe (Quad s) -> IO ()
foobar Nothing = putStrLn "Darn!"
foobar (Just (Quad p _ _ _)) = T.putStrLn . _minerData_account $ _blockPayload_minerData p

lookups :: ClientEnv -> Header -> IO (Maybe (Quad s))
lookups cenv h = runMaybeT $ do
  pl <- MaybeT $ payload cenv h
  let !mi = miner pl
      !bl = block h mi
      !ts = map (transaction bl) $ _blockPayload_transactions pl
  pure $ Quad pl bl mi ts

miner :: BlockPayload -> MinerT (QExpr Sqlite s)
miner pl = Miner
  { _miner_id = default_
  , _miner_account = val_ acc
  , _miner_pred = val_ prd }
  where
    MinerData acc prd _ = _blockPayload_minerData pl

block :: Header -> MinerT (QExpr Sqlite s) -> BlockT (QExpr Sqlite s)
block h m = Block
  { _block_id = default_
  , _block_creationTime = val_ $ _header_creationTime h
  , _block_chainId = val_ $ _header_chainId h
  , _block_height = val_ $ _header_height h
  , _block_hash = val_ $ _header_hash h
  , _block_powHash = val_ $ _header_powHash h
  , _block_target = val_ $ _header_target h
  , _block_weight = val_ $ _header_weight h
  , _block_epochStart = val_ $ _header_epochStart h
  , _block_nonce = val_ $ _header_nonce h
  , _block_miner = pk m }

transaction :: BlockT (QExpr Sqlite s) -> CW.Transaction -> TransactionT (QExpr Sqlite s)
transaction b tx = Transaction
  { _transaction_id = default_
  , _transaction_chainId = _block_chainId b
  , _transaction_block = pk b
  , _transaction_creationTime = val_ . floor $ _chainwebMeta_creationTime mta
  , _transaction_ttl = val_ $ _chainwebMeta_ttl mta
  , _transaction_gasLimit = val_ $ _chainwebMeta_gasLimit mta
  , _transaction_gasPrice = val_ $ _chainwebMeta_gasPrice mta
  , _transaction_sender = val_ $ _chainwebMeta_sender mta
  , _transaction_nonce = val_ $ _pactCommand_nonce cmd
  , _transaction_requestKey = val_ . hashB64U $ CW._transaction_hash tx }
  where
    cmd = CW._transaction_cmd tx
    mta = _pactCommand_meta cmd

payload :: ClientEnv -> Header -> IO (Maybe BlockPayload)
payload cenv h = hush <$> runClientM (payload' "mainnet01" cid hsh) cenv
  where
    cid = ChainId $ _header_chainId h
    hsh = _header_payloadHash h

--------------------------------------------------------------------------------
-- Endpoints

-- TODO Consider ripping this out in favour of vanilla http-client.
-- The approaches are isomorphic, and this usage isn't "the point" of servant.
type PayloadAPI = "chainweb"
  :> "0.0"
  :> Capture "version" ChainwebVersion
  :> "chain"
  :> Capture "chainId" ChainId
  :> "payload"
  :> Capture "BlockPayloadHash" DbHash
  :> Get '[JSON] BlockPayload

api :: Proxy PayloadAPI
api = Proxy

payload' :: ChainwebVersion -> ChainId -> DbHash -> ClientM BlockPayload
payload' = client api

instance ToHttpApiData ChainId where
  toUrlPiece (ChainId cid) = T.pack $ show cid
