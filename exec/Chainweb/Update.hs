{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( updates ) where

import           BasePrelude hiding (delete, insert)
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
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Database.Beam
import           Database.Beam.Sqlite
import           Database.SQLite.Simple (Connection)
import           Network.HTTP.Client hiding (Proxy)

---

-- | A wrapper to massage some types below.
data Quad = Quad !BlockPayload !Block !Miner ![Transaction]

updates :: Manager -> Connection -> Url -> ChainwebVersion -> IO ()
updates m c u v = do
  hs <- runBeamSqlite c . runSelectReturningList $ select prd
  -- TODO Be concurrent via `scheduler`.
  traverse_ (\h -> lookups m u v h >>= writes c h) hs
  where
    prd = all_ $ headers database

-- | Blocks queue for insertion that don't already exist in the database.
-- uniques :: Connection -> Quad -> IO (Maybe Quad)
-- uniques c (Quad p b m ts) = do
--   r <- runBeamSqlite c $ do
--     already <- runSelectReturningOne
--       $ select
--       $ filter_ (\b' -> _block_hash b' ==. _block_hash b)
--       $ all_ (blocks database)
--     undefined
--   undefined

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: Connection -> Header -> Maybe Quad -> IO ()
writes c h (Just q) = writes' c h q >> T.putStrLn ("[SUCCESS] " <> unDbHash (_header_hash h))
writes _ h _ = T.putStrLn $ "[FAILURE] Payload fetch for Block: " <> unDbHash (_header_hash h)

writes' :: Connection -> Header -> Quad -> IO ()
writes' c h (Quad _ b m ts) = runBeamSqlite c $ do
  -- Remove the Header from the work queue --
  runDelete
    $ delete (headers database)
    (\x -> _header_hash x ==. val_ (_header_hash h))
  -- Write the Miner if unique --
  alreadyMined <- runSelectReturningOne
    $ select
    $ filter_ (\mnr -> _miner_account mnr ==. val_ (_miner_account m))
    $ all_ (miners database)
  mnr <- if | isJust alreadyMined -> pure $ fromJust alreadyMined
            | otherwise -> fmap head
              $ runInsertReturningList
              $ insert (miners database)
              $ insertExpressions [m']
  -- Write the Block --
  let !blk = b' mnr
  runInsert
    $ insert (blocks database)
    $ insertExpressions [blk]
  -- Write the Transactions --
  runInsert
    $ insert (transactions database)
    $ insertExpressions
    $ map (t' blk) ts
  where
    m' :: MinerT (QExpr Sqlite s)
    m' = Miner
      { _miner_id = default_
      , _miner_account = val_ $ _miner_account m
      , _miner_pred = val_ $ _miner_pred m }

    b' :: Miner -> BlockT (QExpr Sqlite s)
    b' mnr = Block
      { _block_id = default_
      , _block_creationTime = val_ $ _block_creationTime b
      , _block_chainId = val_ $ _block_chainId b
      , _block_height = val_ $ _block_height b
      , _block_hash = val_ $ _block_hash b
      , _block_powHash = val_ $ _block_powHash b
      , _block_target = val_ $ _block_target b
      , _block_weight = val_ $ _block_weight b
      , _block_epochStart = val_ $ _block_epochStart b
      , _block_nonce = val_ $ _block_nonce b
      , _block_miner = val_ $ pk mnr }

    t' :: BlockT (QExpr Sqlite s) -> Transaction -> TransactionT (QExpr Sqlite s)
    t' blk t = Transaction
      { _transaction_id = default_
      , _transaction_chainId = val_ $ _transaction_chainId t
      , _transaction_block = pk blk
      , _transaction_creationTime = val_ $ _transaction_creationTime t
      , _transaction_ttl = val_ $ _transaction_ttl t
      , _transaction_gasLimit = val_ $ _transaction_gasLimit t
      , _transaction_sender = val_ $ _transaction_sender t
      , _transaction_nonce = val_ $ _transaction_nonce t
      , _transaction_requestKey = val_ $ _transaction_requestKey t }

lookups :: Manager -> Url -> ChainwebVersion -> Header -> IO (Maybe Quad)
lookups m u v h = runMaybeT $ do
  pl <- MaybeT $ payload m u v h
  let !mi = miner pl
      !bl = block h mi
      !ts = map (transaction bl) $ _blockPayload_transactions pl
  pure $ Quad pl bl mi ts

miner :: BlockPayload -> Miner
miner pl = Miner
  { _miner_id = 0
  , _miner_account = acc
  , _miner_pred = prd }
  where
    MinerData acc prd _ = _blockPayload_minerData pl

block :: Header -> Miner -> Block
block h m = Block
  { _block_id = 0
  , _block_creationTime = _header_creationTime h
  , _block_chainId = _header_chainId h
  , _block_height = _header_height h
  , _block_hash = _header_hash h
  , _block_powHash = _header_powHash h
  , _block_target = _header_target h
  , _block_weight = _header_weight h
  , _block_epochStart = _header_epochStart h
  , _block_nonce = _header_nonce h
  , _block_miner = pk m }

transaction :: Block -> CW.Transaction -> Transaction
transaction b tx = Transaction
  { _transaction_id = 0
  , _transaction_chainId = _block_chainId b
  , _transaction_block = pk b
  , _transaction_creationTime = floor $ _chainwebMeta_creationTime mta
  , _transaction_ttl = _chainwebMeta_ttl mta
  , _transaction_gasLimit = _chainwebMeta_gasLimit mta
  -- , _transaction_gasPrice = _chainwebMeta_gasPrice mta
  , _transaction_sender = _chainwebMeta_sender mta
  , _transaction_nonce = _pactCommand_nonce cmd
  , _transaction_requestKey = hashB64U $ CW._transaction_hash tx }
  where
    cmd = CW._transaction_cmd tx
    mta = _pactCommand_meta cmd

--------------------------------------------------------------------------------
-- Endpoints

payload :: Manager -> Url -> ChainwebVersion -> Header -> IO (Maybe BlockPayload)
payload m (Url u) (ChainwebVersion v) h = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://" <> u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh
    cid = T.pack . show $ _header_chainId h
    hsh = unDbHash $ _header_payloadHash h
