{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}

module Chainweb.Worker
  ( writes
  , writeBlock
  , writeBlocks
  , writePayload
  ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           Chainweb.Lookups
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Signer
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Transfer
import           Control.Lens (iforM_)
import           Control.Retry
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.Map as M
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time.Clock (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import qualified Database.Beam.Postgres.Conduit as BPC (runInsert)
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)
import           System.Logger hiding (logg)
import qualified System.Logger as S
---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> [Event] -> [Signer] -> [Transfer] -> IO ()
writes pool b ks ts es ss tf = P.withResource pool $ \c -> withTransaction c $ do
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
        runInsert
          $ insert (_cddb_transfers database) (insertValues tf)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        -- liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
        --   (_block_chainId b)
        --   (_block_height b)
        --   (unDbHash $ _block_hash b)
        --   (map (const '.') ts)

batchWrites
  :: P.Pool Connection
  -> (ChainId, Int, Int)
  -> Maybe FilePath
  -> [Block]
  -> [[T.Text]]
  -> [[Transaction]]
  -> [[Event]]
  -> [[Signer]]
  -> [[Transfer]]
  -> IO ()
batchWrites pool _chunkRange errorLogFile bs kss tss ess sss tfs = P.withResource pool $ \c -> withTransaction c $ do

    let
      logInsertStats :: String -> Int64 -> Int -> IO ()
      logInsertStats entityName inserted attempted =
        forM_ errorLogFile $ \fp -> withFileLogger fp Debug $ do
          S.logg Debug $ "Over range: " <> fromString (show _chunkRange)
          S.logg Debug $ fromString $ printf "%s inserted: %s" entityName (show inserted)
          S.logg Debug $ fromString $ printf "%s attempted: %d" entityName attempted

    runBeamPostgres c $ do
      -- Write the Blocks if unique
      blocksReturned <- BPC.runInsert c
        $ insert (_cddb_blocks database) (insertValues bs)
        $ onConflict (conflictingFields primaryKey) onConflictDoNothing
      -- Write Pub Key many-to-many relationships if unique --

      liftIO $ logInsertStats "Blocks" blocksReturned (length bs)

      let mks = concat $ zipWith (\b ks -> map (MinerKey (pk b)) ks) bs kss

      minerKeysReturned <- BPC.runInsert c
        $ insert (_cddb_minerkeys database) (insertValues mks)
        $ onConflict (conflictingFields primaryKey) onConflictDoNothing

      liftIO $ logInsertStats "Miner Keys" minerKeysReturned (length mks)

    withSavepoint c $ do
      runBeamPostgres c $ do
        -- Write the TXs if unique
        txsReturned <- BPC.runInsert c
          $ insert (_cddb_transactions database) (insertValues $ concat tss)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        liftIO $ logInsertStats "Transactions" txsReturned (length $ concat tss)

        evsReturned <- BPC.runInsert c
          $ insert (_cddb_events database) (insertValues $ concat ess)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        liftIO $ logInsertStats "Events" evsReturned (length $ concat ess)

        signersReturned <- BPC.runInsert c
          $ insert (_cddb_signers database) (insertValues $ concat sss)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        liftIO $ logInsertStats "Signers" signersReturned (length $ concat sss)

        transfersReturned <- BPC.runInsert c
          $ insert (_cddb_transfers database) (insertValues $ concat tfs)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        liftIO $ logInsertStats "Transfers" transfersReturned (length $ concat tfs)


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
          !es = mkBlockEvents (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) pl
          !ss = concat $ map (mkTransactionSigners . fst) (_blockPayloadWithOutputs_transactionsWithOutputs pl)
          version = _nodeInfo_chainwebVer $ _env_nodeInfo env
          !k = bpwoMinerKeys pl
          err = printf "writeBlock failed because we don't know how to work this version %s" version
      withEventsMinHeight version err $ \evMinHeight -> do
          let !tf = mkTransferRows (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) (posixSecondsToUTCTime $ _blockHeader_creationTime bh) pl evMinHeight
          atomicModifyIORef' count (\n -> (n+1, ()))
          writes pool b k t es ss tf
  where
    policy :: RetryPolicyM IO
    policy = exponentialBackoff 250_000 <> limitRetries 3

writeBlocks :: Env -> P.Pool Connection -> Maybe FilePath -> IORef Int -> [BlockHeader] -> IO ()
writeBlocks env pool errorLogFile count bhs = do
    iforM_ blocksByChainId $ \chain (Sum numWrites, bhs', Min minBH, Max maxBH) -> do
      let ff bh = (hashToDbHash $ _blockHeader_payloadHash bh, _blockHeader_hash bh)
      let appendChain c file = file <> "-" <> show c
      retrying policy check (const $ payloadWithOutputsBatch env chain (M.fromList (ff <$> bhs')) id) >>= \case
        Left e -> case appendChain chain <$> errorLogFile of
            Just fp -> withFileLogger fp Error $ do
              S.logg Error $ fromString $ printf "Couldn't fetch payload batch for chain: %d" (unChainId chain) -- TODO: display batch request parameters
              S.logg Error $ fromString $ show e
            Nothing -> do
              logger Error $ fromString $ printf "Couldn't fetch payload batch for chain: %d" (unChainId chain)
              logger Error $ fromString $ show e
        Right pls' -> do
          let !pls = M.fromList pls'
              !ms = _blockPayloadWithOutputs_minerData <$> pls
              !bs = M.intersectionWith (\m bh -> asBlock (asPow bh) m) ms (makeBlockMap bhs')
              !tss = M.intersectionWith (flip mkBlockTransactions) pls bs
              version = _nodeInfo_chainwebVer $ _env_nodeInfo env
              !ess = M.intersectionWith
                  (\pl bh -> mkBlockEvents (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) pl)
                  pls
                  (makeBlockMap bhs')
              !sss = M.intersectionWith (\pl _ -> concat $ mkTransactionSigners . fst <$> _blockPayloadWithOutputs_transactionsWithOutputs pl) pls (makeBlockMap bhs')
              !kss = M.intersectionWith (\p _ -> bpwoMinerKeys p) pls (makeBlockMap bhs')
              err = printf "writeBlocks failed because we don't know how to work this version %s" version
              chunkRange = (chain, minBH, maxBH)
          withEventsMinHeight version err $ \evMinHeight -> do
              let !tfs = M.intersectionWith (\pl bh -> mkTransferRows (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) (posixSecondsToUTCTime $ _blockHeader_creationTime bh) pl evMinHeight) pls (makeBlockMap bhs')
              batchWrites pool chunkRange (appendChain chain <$> errorLogFile) (M.elems bs) (M.elems kss) (M.elems tss) (M.elems ess) (M.elems sss) (M.elems tfs)
              atomicModifyIORef' count (\n -> (n + numWrites, ()))
  where

    makeBlockMap = M.fromList . fmap (\bh -> (_blockHeader_hash bh, bh))

    logger = _env_logger env

    blocksByChainId =
      M.fromListWith mappend
        $ bhs
        <&> \bh -> (_blockHeader_chainId bh, (Sum (1 :: Int), [bh], Min (_blockHeader_height bh), Max (_blockHeader_height bh)))

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
  -> T.Text
  -> UTCTime
  -> BlockPayloadWithOutputs
  -> IO ()
writePayload pool chain blockHash blockHeight version creationTime bpwo = do
  let (cbEvents, txEvents) = mkBlockEvents' blockHeight chain blockHash bpwo
      err = printf "writePayload failed because we don't know how to work this version %s" version
  withEventsMinHeight version err $ \evMinHeight -> do
      let !tfs = mkTransferRows blockHeight chain blockHash creationTime bpwo evMinHeight
      P.withResource pool $ \c ->
        withTransaction c $ do
          runBeamPostgres c $ do
            runInsert
              $ insert (_cddb_events database) (insertValues $ cbEvents ++ concatMap snd txEvents)
              $ onConflict (conflictingFields primaryKey) onConflictDoNothing
            runInsert
              $ insert (_cddb_transfers database) (insertValues tfs)
              $ onConflict (conflictingFields primaryKey) onConflictDoNothing
          withSavepoint c $ runBeamPostgres c $
            forM_ txEvents $ \(reqKey, events) ->
              runUpdate
                $ update (_cddb_transactions database)
                  (\tx -> _tx_numEvents tx <-. val_ (Just $ fromIntegral $ length events))
                  (\tx -> _tx_requestKey tx ==. val_ reqKey)
