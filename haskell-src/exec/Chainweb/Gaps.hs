{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           Chainweb.Lookups
import           Chainweb.Worker (writeBlocks)
import           ChainwebDb.Types.Block
import           ChainwebData.Genesis
import           ChainwebData.Types
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Monad
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
import           System.Logger hiding (logg)
import           System.Exit (exitFailure)
import           Text.Printf

---
gaps :: Env -> FillArgs -> IO ()
gaps env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error "Error querying cut"
      logg Info $ fromString $ show e
    Right cutBS -> gapsCut env args cutBS

gapsCut :: Env -> FillArgs -> ByteString -> IO ()
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
              let ranges = concatMap (createRanges cid) gs
              mapM_ (f logg count cid) ranges
        let gapFiller = do
              race_ (progress logg count totalNumBlocks)
                    (traverseConcurrently_ Par' doChain (M.toList gapsByChain))
              final <- readIORef count
              logg Info $ fromString $ printf "Filled in %d missing blocks." final
        gapFiller
  where
    pool = _env_dbConnPool env
    delay =  _fillArgs_delayMicros args
    gi = mkGenesisInfo $ _env_nodeInfo env
    logg = _env_logger env
    createRanges cid (low, high)
      | low == high = []
      | fromIntegral (genesisHeight (ChainId (fromIntegral cid)) gi) == low = rangeToDescGroupsOf blockHeaderRequestSize (Low $ fromIntegral low) (High $ fromIntegral (high - 1))
      | otherwise = rangeToDescGroupsOf blockHeaderRequestSize (Low $ fromIntegral (low + 1)) (High $ fromIntegral (high - 1))

    f :: LogFunctionIO Text -> IORef Int -> Int64 -> (Low, High) -> IO ()
    f logger count cid (l, h) = do
      let range = (ChainId (fromIntegral cid), l, h)
      headersBetween env range >>= \case
        Left e -> logger Error $ fromString $ printf "ApiError for range %s: %s" (show range) (show e)
        Right [] -> logger Error $ fromString $ printf "headersBetween: %s" $ show range
        Right hs -> writeBlocks env pool count hs
      maybe mempty threadDelay delay

dedupeMinerKeysTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeMinerKeysTable pool logger = do
    logger Info "Deduping minerkeys table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM minerkeys WHERE ctid IN (SELECT\
      \ ctid FROM (SELECT\
      \ block,key,ctid,row_number() OVER (PARTITION BY\
      \ block,key) AS row_num FROM minerkeys) t WHERE t.row_num >1);"

dedupeSignersTable :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeSignersTable pool logger = do
    logger Debug "Deduping signers table"
    P.withResource pool $ \conn ->
      void $ execute_ conn dedupestmt
  where
    dedupestmt =
      "DELETE FROM signers WHERE ctid IN (SELECT ctid\
      \ FROM (SELECT requestkey,idx,ctid,row_number() OVER (PARTITION BY requestkey,idx)\
      \ AS row_num FROM signers) t WHERE t.row_num > 1);"

dedupeTables :: P.Pool Connection -> LogFunctionIO Text -> IO ()
dedupeTables pool logger = do
  -- We don't need to dedupe the following tables because their primary keys
  -- should be unique across any data we might encounter:
  -- events, transactions, blocks
  dedupeMinerKeysTable pool logger
  dedupeSignersTable pool logger

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
