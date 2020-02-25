{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Worker
  ( -- * DB Updates
    writes
    -- * Payload Lookups
  , payload
  , miner
  , txs
  , keys
  ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Api.BlockPayload
import           Chainweb.Api.ChainId
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
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Transaction
import           Data.Aeson (Value(..), decode')
import qualified Data.Pool as P
import qualified Data.Text as T
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Network.HTTP.Client hiding (Proxy)

---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> IO ()
writes pool b ks ts = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Write Pub Key many-to-many relationships if unique --
  runInsert
    $ insert (minerkeys database) (insertValues $ map (MinerKey (pk b)) ks)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the Block if unique --
  runInsert
    $ insert (blocks database) (insertValues [b])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the TXs if unique --
  runInsert
    $ insert (transactions database) (insertValues ts)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
  --   (_block_chainId b)
  --   (_block_height b)
  --   (unDbHash $ _block_hash b)
  --   (map (const '.') ts)

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

--------------------------------------------------------------------------------
-- Endpoints

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

-- | Derive useful database entries from a `Block` and its payload.
txs :: Block -> BlockPayload -> [Transaction]
txs b pl = map (transaction b) $ _blockPayload_transactions pl

miner :: BlockPayload -> MinerData
miner = _blockPayload_minerData

keys :: BlockPayload -> [T.Text]
keys = _minerData_publicKeys . _blockPayload_minerData
