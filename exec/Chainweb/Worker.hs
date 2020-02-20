{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Worker ( worker ) where

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
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction
import           Control.Monad.Trans.Maybe
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import           Data.Aeson (Value(..), decode')
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Network.HTTP.Client hiding (Proxy)

---

worker :: Env -> IO ()
worker e@(Env _ c _ _) = withPool c $ \pool -> do
  hs <- P.withResource pool $ \conn -> runBeamPostgres conn . runSelectReturningList
    $ select
    $ filter_ (isNothing_ . _block_miner)
    $ all_ (blocks database)
  traverseConcurrently_ Par' (\b -> lookups e b >>= writes pool b) hs

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> Maybe (T2 Miner [Transaction]) -> IO ()
writes pool b (Just q) = writes' pool b q
writes _ b _ = T.putStrLn $ "[FAIL] Payload fetch for Block: " <> unDbHash (_block_hash b)

writes' :: P.Pool Connection -> Block -> T2 Miner [Transaction] -> IO ()
writes' pool b (T2 m ts) = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Write the Miner if unique --
  runInsert
    $ insert (miners database) (insertValues [m])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Update the Block to include its new Miner foreign key --
  runUpdate $ save (blocks database) (b { _block_miner = just_ $ pk m })
  -- Write the TXs if unique --
  runInsert
    $ insert (transactions database) (insertValues ts)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
    (_block_chainId b)
    (_block_height b)
    (unDbHash $ _block_hash b)
    (map (const '.') ts)

lookups :: Env -> Block -> IO (Maybe (T2 Miner [Transaction]))
lookups e b = runMaybeT $ do
  pl <- MaybeT $ payload e b
  let !mi = miner pl
      !ts = map (transaction b) $ _blockPayload_transactions pl
  pure $ T2 mi ts

miner :: BlockPayload -> Miner
miner pl = Miner acc prd
  where
    MinerData acc prd _ = _blockPayload_minerData pl

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

payload :: Env -> Block -> IO (Maybe BlockPayload)
payload (Env m _ (Url u) (ChainwebVersion v)) b = do
  req <- parseRequest url
  res <- httpLbs req m
  pure . decode' $ responseBody res
  where
    url = "https://" <> u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh
    cid = T.pack . show $ _block_chainId b
    hsh = unDbHash $ _block_payload b
