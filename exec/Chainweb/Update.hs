{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( updates ) where

import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainId
import           Chainweb.Api.MinerData
import           Chainweb.Api.Transaction
import           Chainweb.Env (ChainwebVersion, Url(..))
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Header
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction hiding (Transaction)
import           Control.Error.Util (hush)
import           Control.Monad.Trans.Maybe
import           Data.Proxy (Proxy(..))
import qualified Data.Text as T
import           Database.Beam
import           Database.Beam.Sqlite (Sqlite, runBeamSqlite)
import           Database.SQLite.Simple (Connection)
import           Network.HTTP.Client (Manager)
import           Servant.API hiding (Header)
import           Servant.Client

---

-- | A wrapper to massage some types below.
data Trio s = Trio
  (BlockT (QExpr Sqlite s))
  (MinerT (QExpr Sqlite s))
  [TransactionT (QExpr Sqlite s)]

updates :: Manager -> Connection -> Url -> IO ()
updates m c (Url u) = do
  putStrLn "Update goes here."
  where
    env = ClientEnv m url Nothing
    url = BaseUrl Https u 443 ""

lookups :: ClientEnv -> Header -> IO (Maybe (Trio s))
lookups cenv h = runMaybeT $ do
  pl <- MaybeT $ payload cenv h
  let !mi = miner pl
      !bl = block h mi
      !ts = map (transaction bl) $ _blockPayload_transactions pl
  pure $ Trio bl mi ts

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
  , _block_powHash = undefined  -- TODO Get from header stream
  , _block_target = val_ $ _header_target h
  , _block_weight = val_ $ _header_weight h
  , _block_epochStart = val_ $ _header_epochStart h
  , _block_nonce = val_ $ _header_nonce h
  , _block_miner = pk m }

transaction :: BlockT (QExpr Sqlite s) -> Transaction -> TransactionT (QExpr Sqlite s)
transaction b = undefined

payload :: ClientEnv -> Header -> IO (Maybe BlockPayload)
payload cenv h = hush <$> runClientM (payload' "mainnet01" cid hsh) cenv
  where
    cid = ChainId $ _header_chainId h
    hsh = _header_payloadHash h

--------------------------------------------------------------------------------
-- Endpoints

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
