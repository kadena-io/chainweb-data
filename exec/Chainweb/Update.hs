{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( updates ) where

import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.PactCommand
import qualified Chainweb.Api.Transaction as CW
import           Chainweb.Database
import           Chainweb.Env (ChainwebVersion(..), Url(..))
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Header
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction
import           Control.Monad.Trans.Maybe
import           Data.Aeson (decode')
import           Data.Foldable (traverse_)
import           Data.Maybe (catMaybes, fromJust, isJust, mapMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Database.Beam
import           Database.Beam.Sqlite
import           Database.SQLite.Simple (Connection)
import           Network.HTTP.Client hiding (Proxy)

---

-- | A wrapper to massage some types below.
data Quad = Quad
  !BlockPayload
  !(BlockT Maybe)
  !(MinerT Maybe)
  ![TransactionT Maybe]

updates :: Manager -> Connection -> Url -> ChainwebVersion -> IO ()
updates m c (Url _) v = do
  hs <- runBeamSqlite c . runSelectReturningList $ select prd
  -- TODO Be concurrent via `scheduler`.
  traverse_ (\h -> lookups m v h >>= writes c h) hs
  where
    prd = all_ $ headers database

writes :: Connection -> Header -> Maybe Quad -> IO ()
writes c h (Just q) = writes' c h q >> T.putStrLn ("[SUCCESS] " <> unDbHash (_header_hash h))
writes _ h _ = T.putStrLn $ "[FAILURE] Payload fetch for Block: " <> unDbHash (_header_hash h)

writes' :: Connection -> Header -> Quad -> IO ()
writes' c h (Quad p b m ts) = runBeamSqlite c $ do
  -- Remove the Header from the work queue --
  runDelete
    $ delete (headers database)
    (\x -> _header_id x ==. val_ (_header_id h))
  -- Write the Miner if unique --
  alreadyMined <- runSelectReturningOne
    $ select
    $ filter_ (\mnr -> _miner_account mnr ==. val_ macc)
    $ all_ (miners database)
  mnr <- if | isJust alreadyMined -> pure $ fromJust alreadyMined
            | otherwise -> fmap head
              $ runInsertReturningList
              $ insert (miners database)
              $ insertExpressions
              $ catMaybes [m']
  -- Write the Block --
  let !blk = b' mnr
  runInsert
    $ insert (blocks database)
    $ insertExpressions
    $ catMaybes [blk]
  -- Write the Transactions --
  runInsert
    $ insert (transactions database)
    $ insertExpressions
    $ mapMaybe (t' blk) ts
  where
    MinerData macc _ _ = _blockPayload_minerData p

    m' :: Maybe (MinerT (QExpr Sqlite s))
    m' = Miner default_
      <$> (val_ <$> _miner_account m)
      <*> (val_ <$> _miner_pred m)

    b' :: Miner -> Maybe (BlockT (QExpr Sqlite s))
    b' mnr = Block default_
      <$> (val_ <$> _block_creationTime b)
      <*> (val_ <$> _block_chainId b)
      <*> (val_ <$> _block_height b)
      <*> (val_ <$> _block_hash b)
      <*> (val_ <$> _block_powHash b)
      <*> (val_ <$> _block_target b)
      <*> (val_ <$> _block_weight b)
      <*> (val_ <$> _block_epochStart b)
      <*> (val_ <$> _block_nonce b)
      <*> pure (val_ $ pk mnr)

    t' :: Maybe (BlockT (QExpr Sqlite s)) -> TransactionT Maybe -> Maybe (TransactionT (QExpr Sqlite s))
    t' blk t = Transaction default_
      <$> (val_ <$> _transaction_chainId t)
      <*> (pk   <$> blk)
      <*> (val_ <$> _transaction_creationTime t)
      <*> (val_ <$> _transaction_ttl t)
      <*> (val_ <$> _transaction_gasLimit t)
      <*> (val_ <$> _transaction_sender t)
      <*> (val_ <$> _transaction_nonce t)
      <*> (val_ <$> _transaction_requestKey t)

lookups :: Manager -> ChainwebVersion -> Header -> IO (Maybe Quad)
lookups m v h = runMaybeT $ do
  pl <- MaybeT $ payload'' m v h
  let !mi = miner pl
      !bl = block h mi
      !ts = map (transaction bl) $ _blockPayload_transactions pl
  pure $ Quad pl bl mi ts

miner :: BlockPayload -> MinerT Maybe
miner pl = Miner
  { _miner_id = Nothing
  , _miner_account = Just acc
  , _miner_pred = Just prd }
  where
    MinerData acc prd _ = _blockPayload_minerData pl

block :: Header -> MinerT Maybe -> BlockT Maybe
block h m = Block
  { _block_id = Nothing
  , _block_creationTime = Just $ _header_creationTime h
  , _block_chainId = Just $ _header_chainId h
  , _block_height = Just $ _header_height h
  , _block_hash = Just $ _header_hash h
  , _block_powHash = Just $ _header_powHash h
  , _block_target = Just $ _header_target h
  , _block_weight = Just $ _header_weight h
  , _block_epochStart = Just $ _header_epochStart h
  , _block_nonce = Just $ _header_nonce h
  , _block_miner = pk m }

transaction :: BlockT Maybe -> CW.Transaction -> TransactionT Maybe
transaction b tx = Transaction
  { _transaction_id = Nothing
  , _transaction_chainId = _block_chainId b
  , _transaction_block = pk b
  , _transaction_creationTime = Just . floor $ _chainwebMeta_creationTime mta
  , _transaction_ttl = Just $ _chainwebMeta_ttl mta
  , _transaction_gasLimit = Just $ _chainwebMeta_gasLimit mta
  -- , _transaction_gasPrice = Just $ _chainwebMeta_gasPrice mta
  , _transaction_sender = Just $ _chainwebMeta_sender mta
  , _transaction_nonce = Just $ _pactCommand_nonce cmd
  , _transaction_requestKey = Just . hashB64U $ CW._transaction_hash tx }
  where
    cmd = CW._transaction_cmd tx
    mta = _pactCommand_meta cmd

--------------------------------------------------------------------------------
-- Endpoints

payload'' :: Manager -> ChainwebVersion -> Header -> IO (Maybe BlockPayload)
payload'' m (ChainwebVersion v) h = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://tetsu.fosskers.ca" <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh
    cid = T.pack . show $ _header_chainId h
    hsh = unDbHash $ _header_payloadHash h
