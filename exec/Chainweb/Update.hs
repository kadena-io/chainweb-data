{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( updates ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.PactCommand
import           Chainweb.Api.Payload
import qualified Chainweb.Api.Transaction as CW
import           Chainweb.Database
import           Chainweb.Env
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Header
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction
import           Control.Monad.Trans.Maybe
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import           Data.Aeson (Value(..), decode')
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Network.HTTP.Client hiding (Proxy)

---

-- | A wrapper to massage some types below.
data Quad = Quad !BlockPayload !Block !Miner ![Transaction]

updates :: Env -> IO ()
updates e@(Env _ c _ _) = withPool c $ \pool -> do
  hs <- P.withResource pool $ \conn -> runBeamPostgres conn . runSelectReturningList
    $ select
    $ all_ (headers database)
  traverseConcurrently_ Par' (\h -> lookups e h >>= writes pool h) hs

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Header -> Maybe Quad -> IO ()
writes pool h (Just q) = writes' pool h q
writes _ h _ = T.putStrLn $ "[FAIL] Payload fetch for Block: " <> unDbHash (_header_hash h)

writes' :: P.Pool Connection -> Header -> Quad -> IO ()
writes' pool h (Quad _ b m ts) = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Remove the Header from the work queue --
  runDelete
    $ delete (headers database)
    (\x -> _header_hash x ==. val_ (_header_hash h))
  -- Write the Miner if unique --
  runInsert
    $ insert (miners database) (insertValues [m])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the Block if unique --
  runInsert
    $ insert (blocks database) (insertValues [b])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the TXs if unique --
  runInsert
    $ insert (transactions database) (insertValues ts)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
    (_block_chainId b)
    (_block_height b)
    (unDbHash $ _block_hash b)
    (map (const '.') ts)

lookups :: Env -> Header -> IO (Maybe Quad)
lookups e h = runMaybeT $ do
  pl <- MaybeT $ payload e h
  let !mi = miner pl
      !bl = block h mi
      !ts = map (transaction bl) $ _blockPayload_transactions pl
  pure $ Quad pl bl mi ts

miner :: BlockPayload -> Miner
miner pl = Miner acc prd
  where
    MinerData acc prd _ = _blockPayload_minerData pl

block :: Header -> Miner -> Block
block h m = Block
  { _block_creationTime = _header_creationTime h
  , _block_chainId = _header_chainId h
  , _block_height = _header_height h
  , _block_hash = _header_hash h
  , _block_parent = _header_parent h
  , _block_powHash = _header_powHash h
  , _block_target = _header_target h
  , _block_weight = _header_weight h
  , _block_epochStart = _header_epochStart h
  , _block_nonce = _header_nonce h
  , _block_miner = just_ $ pk m }

transaction :: Block -> CW.Transaction -> Transaction
transaction b tx = Transaction
  { _tx_chainId = _block_chainId b
  , _tx_block = pk b
  , _tx_creationTime = floor $ _chainwebMeta_creationTime mta
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

--------------------------------------------------------------------------------
-- Endpoints

payload :: Env -> Header -> IO (Maybe BlockPayload)
payload (Env m _ (Url u) (ChainwebVersion v)) h = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://" <> u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh
    cid = T.pack . show $ _header_chainId h
    hsh = unDbHash $ _header_payloadHash h
