{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps, dedupeTables ) where

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker (writeBlocks)
import           ChainwebDb.Types.Block
import           ChainwebData.Genesis
import           ChainwebData.Types
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Monad
import           Control.Monad.Catch
import           Control.Scheduler
import           Data.Bool
import           Data.ByteString.Lazy (ByteString)
import           Data.IORef
import           Data.Int
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import           Data.String
import           Data.Text (Text)
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           System.Logger hiding (logg)
import           System.Exit (exitFailure)
import           Text.Printf

---
gaps :: Env -> GapArgs -> IO ()
gaps env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error "Error querying cut"
      logg Info $ fromString $ show e
    Right cutBS -> gapsCut env args cutBS

gapsCut :: Env -> GapArgs -> ByteString -> IO ()
gapsCut env args cutBS = do
  minHeights <- getAndVerifyMinHeights env cutBS
  getBlockGaps env minHeights >>= \gapsByChain ->
    if null gapsByChain
      then do
        logg Info $ fromString $ printf "No gaps detected."
        logg Info $ fromString $ printf "Either the database is empty or there are truly no gaps!"
      else do
        count <- newIORef 0
        let gapSize (a,b) = case compare a b of
              LT -> b - a - 1
              _ -> 0
            isGap = (> 0) . gapSize
            total = sum $ fmap (sum . map (bool 0 1 . isGap)) gapsByChain :: Int
            totalNumBlocks = fromIntegral $ sum $ fmap (sum . map gapSize) gapsByChain
        logg Info $ fromString $ printf "Filling %d gaps and %d blocks" total totalNumBlocks
        logg Debug $ fromString $ printf "Gaps to fill %s" (show gapsByChain)
        let doChain (cid, gs) = do
              batcher <- newIORef (0,[])
              let ranges = concatMap (createRanges cid) gs
              mapM_ (f logg batcher count cid) ranges
              (_,batch) <- readIORef batcher
              unless (null batch) $ writeBlocks env pool disableIndexesPred count batch
        let gapFiller = do
              race_ (progress logg count totalNumBlocks)
                    (traverseConcurrently_ Par' doChain (M.toList gapsByChain))
              final <- readIORef count
              logg Info $ fromString $ printf "Filled in %d missing blocks." final
        if disableIndexesPred
          then withDroppedIndexes pool logg gapFiller
          else gapFiller
  where
    pool = _env_dbConnPool env
    delay =  _gapArgs_delayMicros args
    disableIndexesPred =  _gapArgs_disableIndexes args
    gi = mkGenesisInfo $ _env_nodeInfo env
    logg = _env_logger env
    createRanges cid (low, high)
      | low == high = []
      | fromIntegral (genesisHeight (ChainId (fromIntegral cid)) gi) == low = rangeToDescGroupsOf blockHeaderRequestSize (Low $ fromIntegral low) (High $ fromIntegral (high - 1))
      | otherwise = rangeToDescGroupsOf blockHeaderRequestSize (Low $ fromIntegral (low + 1)) (High $ fromIntegral (high - 1))

    f :: LogFunctionIO Text -> IORef (Int, [BlockHeader]) -> IORef Int -> Int64 -> (Low, High) -> IO ()
    f logger batcher count cid (l, h) = do
      let range = (ChainId (fromIntegral cid), l, h)
      --logger Debug $ fromString $ printf "Processing range %s" (show range)
      headersBetween env range >>= \case
        Left e -> logger Error $ fromString $ printf "ApiError for range %s: %s" (show range) (show e)
        Right [] -> logger Error $ fromString $ printf "headersBetween: %s" $ show range
        Right hs -> do
          (size, batch) <- readIORef batcher
          let lenhs = length hs
          case compare (size + lenhs) 1000 of
            LT -> atomicModifyIORef' batcher (\(s,b) -> ((s + lenhs, b ++ hs), ()))
            EQ -> do
              writeBlocks env pool disableIndexesPred count (batch ++ hs)
              atomicModifyIORef' batcher (\_ -> ((0,[]), ()))
            GT -> do
              case splitAt (size + lenhs - 1000) hs of
                (keep,towrite) -> do
                  writeBlocks env pool disableIndexesPred count (batch ++ towrite)
                  atomicModifyIORef batcher (\_ -> ((size + lenhs - 1000, keep), ()))
      maybe mempty threadDelay delay

listIndexes :: P.Pool Connection -> LogFunctionIO Text -> IO [(String, String)]
listIndexes pool logger = P.withResource pool $ \conn -> do
    res <- query_ conn qry
    forM_ res $ \(_,name) -> do
      logger Debug "index name"
      logger Debug $ fromString name
    return res
  where
    qry =
      "SELECT tablename, indexname FROM pg_indexes WHERE schemaname='public';"

dropIndexes :: P.Pool Connection -> [(String, String)] -> IO ()
dropIndexes pool indexinfos = forM_ indexinfos $ \(tablename, indexname) -> P.withResource pool $ \conn ->
  execute_ conn $ Query $ fromString $ printf "ALTER TABLE %s DROP CONSTRAINT %s CASCADE;" tablename indexname

dedupeEventsTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeEventsTable pool logger = do
    logger Info "Deduping events table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM events WHERE (requestkey,chainid,height,idx,ctid) IN (SELECT\
      \ requestkey,chainid,height,idx,ctid FROM (SELECT\
      \ requestkey,chainid,height,idx,ctid,row_number() OVER (PARTITION BY\
      \ requestkey,chainid,height,idx) AS row_num FROM events) t WHERE t.row_num >1);"

dedupeBlocksTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeBlocksTable pool logger = do
    logger Info "Deduping blocks table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM blocks WHERE (hash,ctid) IN (SELECT\
      \ hash,ctid FROM (SELECT\
      \ hash,ctid,row_number() OVER (PARTITION BY\
      \ hash) AS row_num FROM blocks) t WHERE t.row_num >1);"

dedupeMinerKeysTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeMinerKeysTable pool logger = do
    logger Info "Deduping minerkeys table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM minerkeys WHERE (block,key,ctid) IN (SELECT\
      \ block,key,ctid FROM (SELECT\
      \ block,key,ctid,row_number() OVER (PARTITION BY\
      \ block,key) AS row_num FROM minerkeys) t WHERE t.row_num >1);"

dedupeTransactionsTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeTransactionsTable pool logger = do
    logger Info "Deduping transactions table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM transactions WHERE (requestkey,block,ctid) IN (SELECT\
      \ requestkey,block,ctid FROM (SELECT\
      \ requestkey,block,ctid,row_number() OVER (PARTITION BY\
      \ requestkey,block) AS row_num FROM transactions) t WHERE t.row_num >1);"

dedupeSignersTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeSignersTable pool logger = do
    logger Debug "Deduping signers table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM signers WHERE (requestkey,idx,ctid) IN (SELECT requestkey,idx,ctid\
      \ FROM (SELECT requestkey,idx,ctid,row_number() OVER (PARTITION BY requestkey,idx)\
      \ AS row_num FROM signers) t WHERE t.row_num > 1);"

dedupeTables :: Env -> IO ()
dedupeTables env = do
  let pool = _env_dbConnPool env
      logger = _env_logger env
  dedupeTransactionsTable pool logger
  dedupeEventsTable pool logger
  dedupeBlocksTable pool logger
  dedupeMinerKeysTable pool logger
  dedupeSignersTable pool logger

withDroppedIndexes :: P.Pool Connection -> LogFunctionIO Text -> IO a -> IO a
withDroppedIndexes pool logger action = do
    indexInfos <- listIndexes pool logger
    fmap fst $ generalBracket (dropIndexes pool indexInfos) release (const action)
  where
    release _ = \case
      ExitCaseSuccess _ -> return () -- dedupeTables pool logger
      _ -> return ()

getBlockGaps :: Env -> M.Map Int64 (Maybe Int64) -> IO (M.Map Int64 [(Int64,Int64)])
getBlockGaps env existingMinHeights = withDbDebug env Debug $ do
    let toMap = M.fromListWith (<>) . map (\(cid,a,b) -> (cid,[(a,b)]))
    foundGaps <- fmap toMap $ runSelectReturningList $ selectWith $ do
      foundGaps <- selecting $
        withWindow_ (\b -> frame_ (partitionBy_ (_block_chainId b)) (orderPartitionBy_ $ asc_ $ _block_height b) noBounds_)
                    (\b w -> (_block_chainId b, _block_height b, lead_ (_block_height b) (val_ (1 :: Int64)) `over_` w))
                    (all_ $ _cddb_blocks database)
      pure $ orderBy_ (\(cid,a,_) -> (desc_ a, asc_ cid)) $ do
        res@(_,a,b) <- reuse foundGaps
        guard_ ((b - a) >. val_ 1)
        pure res
    let minHeights = M.intersectionWith maybeAppendGenesis existingMinHeights
          $ fmap toInt64 $ M.mapKeys toInt64 genesisInfo
    unless (M.null minHeights) (liftIO $ logg Debug $ fromString $ "minHeight: " <> show minHeights)
    pure $ if M.null foundGaps
      then M.mapMaybe (fmap pure) minHeights
      else M.intersectionWith addStart minHeights foundGaps
  where
    logg level = _env_logger env level . fromString
    genesisInfo = getGenesisInfo $ mkGenesisInfo $ _env_nodeInfo env
    toInt64 a = fromIntegral a :: Int64
    maybeAppendGenesis mMin genesisheight =
      case mMin of
        Just min' -> case compare genesisheight min' of
          GT -> Nothing
          _ -> Just (genesisheight, min')
        Nothing -> Nothing
    addStart mr xs = case mr of
        Nothing -> xs
        Just r@(a,b)
          | a == b -> xs
          | otherwise -> r : xs

chainMinHeights :: Pg (M.Map Int64 (Maybe Int64))
chainMinHeights =
  fmap M.fromList
  $ runSelectReturningList
  $ select
  $ aggregate_ (\b -> (group_ (_block_chainId b), min_ (_block_height b))) (all_ $ _cddb_blocks database)

getAndVerifyMinHeights :: Env -> ByteString -> IO (M.Map Int64 (Maybe Int64))
getAndVerifyMinHeights env cutBS = do
  minHeights <- withDbDebug env Debug chainMinHeights
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      count = length minHeights
      cids = atBlockHeight curHeight $ _env_chainsAtHeight env
      logg = _env_logger env
  when (count /= length cids) $ do
    logg Error $ fromString $ printf "%d chains have, but we expected %d." count (length cids)
    logg Error $ fromString $ printf "Please run 'listen' or 'server' first, and ensure that a block has been received on each chain."
    logg Error $ fromString $ printf "That should take about a minute, after which you can rerun this command."
    exitFailure
  return minHeights
