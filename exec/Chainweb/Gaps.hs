{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

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
import           Control.Exception
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
  let curHeight = fromIntegral $ cutMaxHeight cutBS
      cids = atBlockHeight curHeight $ _env_chainsAtHeight env
  getBlockGaps env >>= \gapsByChain ->
    if null gapsByChain
      then do
        logg Info $ fromString $ printf "No gaps detected.\n"
        logg Info $ fromString $ printf "Either the database is empty or there are truly no gaps!\n"
      else do
        when (M.size gapsByChain /= length cids) $ do
          logg Error $ fromString $ printf "%d chains have block data, but we expected %d.\n" (M.size gapsByChain) (length cids)
          logg Error $ fromString $ printf "Please run a 'listen' first, and ensure that each chain has a least one block.\n"
          logg Error $ fromString $ printf "That should take about a minute, after which you can rerun 'gaps' separately.\n"
          exitFailure
        count <- newIORef 0
        sampler <- newIORef 0
        let strat = maybe Seq (const Par') delay
            total = sum $ fmap length gapsByChain
        logg Info $ fromString $ printf "Filling %d gaps\n" total
        bool id (withDroppedIndexes env) disableIndexesPred $ race_ (progress logg count total) $
          traverseMapConcurrently_ Par' (\cid -> traverseConcurrently_ strat (f logg count sampler cid) . concatMap createRanges) gapsByChain
        final <- readIORef count
        logg Info $ fromString $ printf "Filled in %d missing blocks.\n" final
  where
    pool = _env_dbConnPool env
    delay =  _gapArgs_delayMicros args
    disableIndexesPred =  _gapArgs_disableIndexes args
    logg = _env_logger env
    traverseMapConcurrently_ comp g m =
      withScheduler_ comp $ \s -> scheduleWork s $ void $ M.traverseWithKey (\k -> scheduleWork s . void . g k) m
    createRanges (l,h)
       | l < h =
          let xs = [h, h - 360 .. l]
          in case zip xs (drop 1 xs) of
              (a:as) -> a : map (\(s,t) -> (s - 1, t)) as
              _ -> []
       | otherwise = []
    f :: LogFunctionIO Text -> IORef Int -> IORef Int -> Int64 -> (Int64, Int64) -> IO ()
    f logger count sampler cid (l, h) = do
      let range = (ChainId (fromIntegral cid), Low (fromIntegral l), High (fromIntegral h))
      headersBetween env range >>= \case
        Left e -> logger Error $ fromString $ printf "ApiError for range %s: %s\n" (show range) (show e)
        Right [] -> logger Error $ fromString $ printf "headersBetween: %s\n" $ show range
        Right hs -> writeBlocks env pool disableIndexesPred count sampler hs
      maybe mempty threadDelay delay

listIndexes :: Env -> IO [(String, String, String)]
listIndexes env = P.withResource (_env_dbConnPool env) $ \conn -> do
    res <- query_ conn qry
    forM_ res $ \(_,name,definition) -> do
      putStrLn "index name"
      putStrLn name
      putStrLn "index definitions"
      putStrLn definition
    return res
  where
    qry =
      "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public';"

dropIndexes :: Env -> [(String, String, String)] -> IO ()
dropIndexes env indexinfos = forM_ indexinfos $ \(tablename, indexname, _) -> P.withResource (_env_dbConnPool env) $ \conn ->
  execute_ conn $ Query $ fromString $ printf "ALTER TABLE %s DROP CONSTRAINT %s ;" tablename indexname

withDroppedIndexes :: Env -> IO a -> IO a
withDroppedIndexes env action = do
    indexInfos <- listIndexes env
    bracket (dropIndexes env indexInfos) (const $ recreateIndexes env indexInfos) (const action)

recreateIndexes :: Env -> [(String, String, String)] -> IO ()
recreateIndexes env indexinfos = forM_ indexinfos $ \(_,_,indexdef) -> P.withResource (_env_dbConnPool env) $ \conn ->
  execute_ conn (Query $ fromString $ indexdef <> ";")

getBlockGaps :: Env -> IO (M.Map Int64 [(Int64,Int64)])
getBlockGaps env = P.withResource (_env_dbConnPool env) $ \c -> do
    runBeamPostgresDebug (liftIO . logg Debug) c $ do
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
      minHeights' <- chainMinHeights
      let minHeights =
            M.intersectionWith
              maybeAppendGenesis
              minHeights'
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
