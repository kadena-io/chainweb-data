{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}

module Chainweb.Worker
  ( writes
  , writeBlock
  ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.Hash
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Event
import           Control.Retry
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)

---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> [Event] -> IO ()
writes pool b ks ts es = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Write Pub Key many-to-many relationships if unique --
  runInsert
    $ insert (_cddb_minerkeys database) (insertValues $ map (MinerKey (pk b)) ks)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the Block if unique --
  runInsert
    $ insert (_cddb_blocks database) (insertValues [b])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the TXs if unique --
  runInsert
    $ insert (_cddb_transactions database) (insertValues ts)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the events if unique --
  runInsert
    $ insert (_cddb_events database) (insertValues es)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
  --   (_block_chainId b)
  --   (_block_height b)
  --   (unDbHash $ _block_hash b)
  --   (map (const '.') ts)

asPow :: BlockHeader -> PowHeader
asPow bh = PowHeader bh (T.decodeUtf8 . B16.encode . B.reverse . unHash $ powHash bh)

-- | Given a single `BlockHeader`, look up its payload and write everything to
-- the database.
writeBlock :: Env -> P.Pool Connection -> IORef Int -> BlockHeader -> IO ()
writeBlock e pool count bh = do
  let !pair = T2 (_blockHeader_chainId bh) (hashToDbHash $ _blockHeader_payloadHash bh)
  retrying policy check (const $ payloadWithOutputs e pair) >>= \case
    Nothing -> printf "[FAIL] Couldn't fetch parent for: %s\n"
      (hashB64U $ _blockHeader_hash bh)
    Just pl -> do
      let !m = _blockPayloadWithOutputs_minerData pl
          !b = asBlock (asPow bh) m
          !t = mkBlockTransactions b pl
          !es = mkBlockEvents b pl
          !k = bpwoMinerKeys pl
      atomicModifyIORef' count (\n -> (n+1, ()))
      writes pool b k t es
  where
    policy :: RetryPolicyM IO
    policy = exponentialBackoff 250_000 <> limitRetries 3

    check :: RetryStatus -> Maybe a -> IO Bool
    check _ = pure . isNothing
