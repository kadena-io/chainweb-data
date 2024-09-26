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
import           ChainwebDb.Types.Verifier
import           Control.Lens (iforM_)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.Map as M
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time.Clock (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)
import           Database.PostgreSQL.Simple.Transaction (withTransaction,withSavepoint)

---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> [Event] -> [Signer] -> [Transfer] -> [Verifier] -> IO ()
writes pool b ks ts es ss tf vs = P.withResource pool $ \c -> withTransaction c $ do
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
        runInsert
          $ insert (_cddb_verifiers database) (insertValues vs)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing
        -- liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
        --   (_block_chainId b)
        --   (_block_height b)
        --   (unDbHash $ _block_hash b)
        --   (map (const '.') ts)

batchWrites
  :: P.Pool Connection
  -> [Block]
  -> [[T.Text]]
  -> [[Transaction]]
  -> [[Event]]
  -> [[Signer]]
  -> [[Transfer]]
  -> [[Verifier]]
  -> IO ()
batchWrites pool bs kss tss ess sss tfs vss = P.withResource pool $ \c -> withTransaction c $ do

    runBeamPostgres c $ do
      -- Write the Blocks if unique
      runInsert
        $ insert (_cddb_blocks database) (insertValues bs)
        $ onConflict (conflictingFields primaryKey) onConflictDoNothing
      -- Write Pub Key many-to-many relationships if unique --

      let mks = concat $ zipWith (\b ks -> map (MinerKey (pk b)) ks) bs kss

      runInsert
        $ insert (_cddb_minerkeys database) (insertValues mks)
        $ onConflict (conflictingFields primaryKey) onConflictDoNothing

    withSavepoint c $ do
      runBeamPostgres c $ do
        -- Write the TXs if unique
        runInsert
          $ insert (_cddb_transactions database) (insertValues $ concat tss)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        runInsert
          $ insert (_cddb_events database) (insertValues $ concat ess)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        runInsert
          $ insert (_cddb_signers database) (insertValues $ concat sss)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        runInsert
          $ insert (_cddb_transfers database) (insertValues $ concat tfs)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing

        runInsert
          $ insert (_cddb_verifiers database) (insertValues $ concat vss)
          $ onConflict (conflictingFields primaryKey) onConflictDoNothing


asPow :: BlockHeader -> PowHeader
asPow bh = PowHeader bh (T.decodeUtf8 . B16.encode . B.reverse . unHash $ powHash bh)

-- | Given a single `BlockHeader`, look up its payload and write everything to
-- the database.
writeBlock :: Env -> P.Pool Connection -> IORef Int -> (BlockHeader, BlockPayloadWithOutputs) -> IO ()
writeBlock env pool count (bh, pwo) = do
  let !m = _blockPayloadWithOutputs_minerData pwo
      !b = asBlock (asPow bh) m
      !t = mkBlockTransactions b pwo
      !es = mkBlockEvents (fromIntegral $ _blockHeader_height bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) pwo
      !ss = concat $ map (mkTransactionSigners . fst) (_blockPayloadWithOutputs_transactionsWithOutputs pwo)
      version = _nodeInfo_chainwebVer $ _env_nodeInfo env
      !k = bpwoMinerKeys pwo
      eventErr = printf "writeBlock failed to write event and transfer rows because we don't know how to work this version %s" version
      verifierErr = printf "writeBlock failed to write verifier row because we don't know how to work this version %s" version
  withEventsMinHeight version eventErr $ \evMinHeight ->
    withVerifiersMinHeight version verifierErr $ \verifierMinHeight -> do
      let currentHeight = fromIntegral $ _blockHeader_height bh
          !tf = mkTransferRows currentHeight (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) (posixSecondsToUTCTime $ _blockHeader_creationTime bh) pwo evMinHeight
      let !vs = concat $ map (mkTransactionVerifiers currentHeight verifierMinHeight . fst) (_blockPayloadWithOutputs_transactionsWithOutputs pwo)

      atomicModifyIORef' count (\n -> (n+1, ()))
      writes pool b k t es ss tf vs

writeBlocks :: Env -> P.Pool Connection -> IORef Int -> [(BlockHeader, BlockPayloadWithOutputs)] -> IO ()
writeBlocks env pool count blocks = do
    iforM_ blocksByChainId $ \_chain (Sum numWrites, bhs') -> do
      let
        pls = M.fromList [ ((_blockHeader_hash bh), pwo) | (bh, pwo) <- bhs' ]
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
        eventErr = printf "writeBlocks failed to write event and transfer rows because we don't know how to work this version %s" version
        verifierErr = printf "writeBlocks failed to write verifier row because we don't know how to work this version %s" version
      withEventsMinHeight version eventErr $ \evMinHeight ->
        withVerifiersMinHeight version verifierErr $ \verifierMinHeight -> do
          let currentHeight bh = fromIntegral $ _blockHeader_height bh
              !tfs = M.intersectionWith (\pl bh -> mkTransferRows (currentHeight bh) (_blockHeader_chainId bh) (DbHash $ hashB64U $ _blockHeader_hash bh) (posixSecondsToUTCTime $ _blockHeader_creationTime bh) pl evMinHeight) pls (makeBlockMap bhs')
              !vss = M.intersectionWith (\pl bh -> concat $ mkTransactionVerifiers (currentHeight bh) verifierMinHeight . fst <$> _blockPayloadWithOutputs_transactionsWithOutputs pl) pls (makeBlockMap bhs')

          batchWrites pool (M.elems bs) (M.elems kss) (M.elems tss) (M.elems ess) (M.elems sss) (M.elems tfs) (M.elems vss)
          atomicModifyIORef' count (\n -> (n + numWrites, ()))
  where

    makeBlockMap = M.fromList . fmap (\(bh, _) -> (_blockHeader_hash bh, bh))

    blocksByChainId =
      M.fromListWith mappend
        $ blocks
        <&> \(bh, pwo) -> (_blockHeader_chainId bh, (Sum (1 :: Int), [(bh, pwo)]))

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
      eventErr = printf "writePayload failed to insert event and transfer rows because we don't know how to work this version %s" version
      verifierErr = printf "writePayload failed to insert verifier row because we don't know how to work this version %s" version
  withEventsMinHeight version eventErr $ \evMinHeight ->
      withVerifiersMinHeight version verifierErr $ \verifierMinHeight -> do
        let !tfs = mkTransferRows blockHeight chain blockHash creationTime bpwo evMinHeight
        let !vss = concat $ map (mkTransactionVerifiers blockHeight verifierMinHeight . fst) $ _blockPayloadWithOutputs_transactionsWithOutputs bpwo
        P.withResource pool $ \c ->
          withTransaction c $ do
            runBeamPostgres c $ do
              runInsert
                $ insert (_cddb_events database) (insertValues $ cbEvents ++ concatMap snd txEvents)
                $ onConflict (conflictingFields primaryKey) onConflictDoNothing
              runInsert
                $ insert (_cddb_transfers database) (insertValues tfs)
                $ onConflict (conflictingFields primaryKey) onConflictDoNothing
              -- TODO: This might be necessary. Will need to think about this further
              runInsert
                $ insert (_cddb_verifiers database) (insertValues vss)
                $ onConflict (conflictingFields primaryKey) onConflictDoNothing
            withSavepoint c $ runBeamPostgres c $
              forM_ txEvents $ \(reqKey, events) ->
                runUpdate
                  $ update (_cddb_transactions database)
                    (\tx -> _tx_numEvents tx <-. val_ (Just $ fromIntegral $ length events))
                    (\tx -> _tx_requestKey tx ==. val_ reqKey)
