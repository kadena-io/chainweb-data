{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}

module Chainweb.Worker
  ( writes
  , writeBlock
  , writePayload
  ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Signer
import           ChainwebDb.Types.Transaction
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
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)
import           System.Logger hiding (logg)
---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> [Event] -> [Signer] -> IO ()
writes pool b ks ts es ss = P.withResource pool $ \c -> withTransaction c $ do
     runBeamPostgres c $ do
        -- Write the Block if unique --
        runInsert
          $ insert (_cddb_blocks database) (insertValues [b])
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        -- Write Pub Key many-to-many relationships if unique --
        runInsert
          $ insert (_cddb_minerkeys database) (insertValues $ map (MinerKey (pk b)) ks)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
     {- We should still be able to write the block & miner keys data if writing
     either the transaction or event data somehow fails. -}
     withSavepoint c $ runBeamPostgres c $ do
        -- Write the TXs if unique --
        runInsert
          $ insert (_cddb_transactions database) (insertValues ts)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        -- Write the events if unique --
        runInsert
          $ insert (_cddb_events database) (insertValues es)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        runInsert
          $ insert (_cddb_signers database) (insertValues ss)
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
writeBlock env pool count bh = do
  let !pair = T2 (_blockHeader_chainId bh) (hashToDbHash $ _blockHeader_payloadHash bh)
      logg = _env_logger env
  retrying policy check (const $ payloadWithOutputs env pair) >>= \case
    Left e -> do
      logg Error $ fromString $ printf "Couldn't fetch parent for: %s"
        (hashB64U $ _blockHeader_hash bh)
      logg Info $ fromString $ show e
    Right pl -> do
      let !m = _blockPayloadWithOutputs_minerData pl
          !b = asBlock (asPow bh) m
          !t = mkBlockTransactions b pl
          !es = mkBlockEvents (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_parent bh) pl
          !ss = concat $ map (mkTransactionSigners . fst) (_blockPayloadWithOutputs_transactionsWithOutputs pl)
          !k = bpwoMinerKeys pl
      atomicModifyIORef' count (\n -> (n+1, ()))
      writes pool b k t es ss
  where
    policy :: RetryPolicyM IO
    policy = exponentialBackoff 250_000 <> limitRetries 3

    check :: RetryStatus -> Either ApiError a -> IO Bool
    check _ ev = pure $
      case ev of
        Left e -> apiError_type e == RateLimiting
        _ -> False

-- | Writes all of the events from a block payload to the events table.
writePayload
  :: P.Pool Connection
  -> ChainId
  -> DbHash BlockHash
  -> Int64
  -> BlockPayloadWithOutputs
  -> IO ()
writePayload pool chain blockHash blockHeight bpwo = do
  let (cbEvents, txEvents) = mkBlockEvents' blockHeight chain blockHash bpwo

  P.withResource pool $ \c ->
    withTransaction c $ do
      runBeamPostgres c $
        runInsert
          $ insert (_cddb_events database) (insertValues $ cbEvents ++ concatMap snd txEvents)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
      withSavepoint c $ runBeamPostgres c $
        forM_ txEvents $ \(reqKey, events) ->
          runUpdate
            $ update (_cddb_transactions database)
              (\tx -> _tx_numEvents tx <-. val_ (Just $ fromIntegral $ length events))
              (\tx -> _tx_requestKey tx ==. val_ reqKey)
