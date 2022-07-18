{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Chainweb.BackfillTransfers where

import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           ChainwebDb.Types.Common
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Transaction

import           Control.Concurrent.Async (race_)
import           Control.Lens hiding ((<.), reuse)
import           Control.Scheduler hiding (traverse_)

import qualified Data.Aeson as A
import           Data.Aeson.Lens
import           Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Pool as P
import           Data.Scientific (toRealFloat)
import           Data.Text (Text)
import           Data.Time.Clock

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.Beam.Query.DataTypes ()
-- import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

-- backfill an empty transfers table (steps)
-- 1. check if transfers table is actually empty. If so, wait until server fills some rows near "top" to start backfill
-- 2. check if events table has any coinbase gaps. If so,  tell user to fill those gaps
-- 3. Fill from last known max height on each chain all the way back to events activation height(s)
backfillTransfersCut :: Env -> Bool -> BackfillArgs -> IO ()
backfillTransfersCut env disableIndexesPred args = do

    withDb env (runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_transfers database) $> as_ @Int32 (val_ 1))) >>= \case
      Just False -> do
        logg Error "Run the server command to start filling this table with some entries!"
        exitSuccess
      Just True -> logg Info "transfers table already exists. Great!"
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as transfers);) failed somehow."

    withDb env (runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_events database) $> as_ @Int32 (val_ 1))) >>= \case
      Just True -> logg Info "events table already exists. Great!"
      Just False -> do
        logg Error "events table does not exist"
        exitFailure
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as events);) failed somehow."

    let startingHeight :: Int64 = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
          "mainnet01" -> 1_722_501
          "testnet04" -> 1_261_001
          _ -> error "Chainweb version: Unknown"

    minHeights <- withDbDebug env Debug chainMinHeights
    let checkMinHeights =
          maybe False (== _nodeInfo_numChains (_env_nodeInfo env)) . foldr go (Just 0)
        go (_,mh) (Just acc) =
          case mh of
            Just _ ->  Just $ acc + 1
            Nothing -> Nothing
        go _ Nothing = Nothing
    unless (checkMinHeights minHeights) $ do
      logg Error "Make sure transfers table has an entry for every chain id!"
      exitFailure
    let minMinHeights = minimum $ mapMaybe snd $ minHeights
    -- get number of entries that actually need to be filled
    trueTotal <- withDbDebug env Debug $ runSelectReturningOne $ select $ aggregate_ (\_ -> as_ @Int64 countAll_) (eventSelector startingHeight minMinHeights Nothing)
    unless (isJust trueTotal) $ die "Cannot get the number of entries needed to fill transfers table"
    logg Info $ fromString $ printf  "Filling transfers table on all chains from height %d to height %d." startingHeight minMinHeights
    mapM_ (\(cid, h) -> logg Info $ fromString $ printf "Filling transfers table on chain %d from height %d to height %d." cid minMinHeights (fromJust h)) minHeights

    ref <- newIORef 0
    let _strat = case delay of
          Nothing -> Par'
          Just _ -> Seq
    bool id (withDroppedIndexes pool) disableIndexesPred $
      catch
        (race_ (progress logg ref (fromIntegral $ fromJust $ trueTotal)) $ loopOnJust 0 (transferInserter ref startingHeight minMinHeights chunkSize))
        $ \(e :: SomeException) -> do
            printf "\nDepending on the error you may need to run backfill for events\n%s\n" (show e)
            exitFailure
  where
    delay = _backfillArgs_delayMicros args
    logg = _env_logger env
    pool = _env_dbConnPool env
    loopOnJust initValue f = flip fix initValue $ \looper value -> f value >>= maybe (pure ()) looper
    chunkSize = maybe 1000 fromIntegral $ _backfillArgs_chunkSize args
    transferInserter :: IORef Int -> Int64 -> Int64 -> Integer -> Integer -> IO (Maybe Integer)
    transferInserter count startingHeight endingHeight lim off = bool id (trackTime "Transfer actions" logg) True $ do
        P.withResource pool $ \c -> withTransaction  c $ runBeamPostgres c $ do
          (rkEvs, coinbaseEvs) <- fmap splitEvents $ runSelectReturningList $ select $ eventSelector' startingHeight endingHeight Nothing lim off
          unless (null rkEvs) $ do
              iforM_ (groupEvByChain rkEvs) $ \cid evs -> do
                let minHeightRK = minimum $ map _ev_height evs
                    maxHeightRK = maximum $ map _ev_height evs
                tsRK <- runSelectReturningList $ select $ creationTimesByRequestKeysNonCoinbase minHeightRK maxHeightRK cid evs
                let tfsRK = createTransfers evs tsRK
                runInsert $
                  insert (_cddb_transfers database) (insertValues tfsRK)
                  $ bool (onConflict (conflictingFields primaryKey) onConflictDoNothing) onConflictDefault disableIndexesPred
                liftIO $ atomicModifyIORef' count (\cnt -> (cnt + length tfsRK, ()))
          unless (null coinbaseEvs) $ do
              iforM_ (groupEvByChain coinbaseEvs) $ \cid evs -> do
                let minHeightCB = minimum $ map _ev_height evs
                    maxHeightCB = maximum $ map _ev_height evs
                tsCB <- runSelectReturningList $ select $ creationTimesByRequestKeysCoinbase minHeightCB maxHeightCB cid evs
                let tfsCB = createTransfers evs tsCB
                runInsert $
                  insert (_cddb_transfers database) (insertValues tfsCB)
                  $ bool (onConflict (conflictingFields primaryKey) onConflictDoNothing) onConflictDefault disableIndexesPred
                liftIO $ atomicModifyIORef' count (\cnt -> (cnt + length tfsCB, ()))
          let length' = liftA3 bool (fromIntegral . length) (const 0) null
          return $ if null coinbaseEvs && null rkEvs
            then Nothing
            else Just $ off + length' rkEvs + length' coinbaseEvs

    -- _transferInserter count startingHeight endingHeight Nothing lim off _chunkSize = bool id (trackTime "Transfer actions" logg) True $ do
    --     P.withResource pool $ \c -> withTransaction c $ runBeamPostgres c $ do
    --       evs <- runSelectReturningList $ select $ eventSelector' startingHeight endingHeight Nothing lim off
    --       let (rkEvs, coinbaseEvs) = splitEvents evs
    --       -- I'm grouping the events by chainid because there could request key collisons across chains
    --       iforM_ (groupEvByChain rkEvs) $ \cid evs' -> do
    --         -- maximumBy/minimumBy (comparing _ev_height) evs' has type EventT Identity instead of Int64 ... strange
    --         let minHeight = minimum $ map _ev_height evs'
    --             maxHeight = maximum $ map _ev_height evs'
    --         ts <- runSelectReturningList $ select $ creationTimesByRequestKeysNonCoinbase minHeight maxHeight cid evs'
    --         let tfs = createTransfers evs' ts
    --         runInsert $
    --           insert (_cddb_transfers database) (insertValues tfs)
    --           $ onConflict (conflictingFields primaryKey) onConflictDoNothing
    --         liftIO $ do
    --           atomicModifyIORef' count (\cnt -> (cnt + length tfs, ()))
    --           logg Info "transfer insertion completed and accounted for"
    --       -- I'm grouping the events by chainid because there could request key collisons across chains
    --       iforM_ (groupEvByChain coinbaseEvs) $ \cid evs' -> do
    --         let minHeight = minimum $ map _ev_height evs'
    --             maxHeight = maximum $ map _ev_height evs'
    --         ts <- runSelectReturningList $ select $ creationTimesByRequestKeysCoinbase minHeight maxHeight cid evs'
    --         let tfs = createTransfers evs' ts
    --         runInsert $
    --           insert (_cddb_transfers database) (insertValues tfs)
    --           $ onConflict (conflictingFields primaryKey) onConflictDoNothing
    --         liftIO $ do
    --           atomicModifyIORef' count (\cnt -> (cnt + length tfs, ()))
    --           logg Info "transfer insertion completed and accounted for"
    --     return Nothing

trackTime :: String -> LogFunctionIO Text -> IO a -> IO a
trackTime s logg action = do
    t1 <- getCurrentTime
    val <- action
    t2 <- getCurrentTime
    logg Info $ fromString $ printf "%s actions took %s" s (show $ diffUTCTime t2 t1)
    return val


fillRegions :: Int64 -> [(Int64,Int64)] -> [Chunk Int64]
fillRegions startingHeight minHeights = maybe id ((:)) largestRegion $ getRegions (drop 1 sortedHeights)
  where
    minMinHeights = snd $ head sortedHeights
    sortedHeights = sortOn snd minHeights
    largestRegion
      | startingHeight /= minMinHeights = Just $ AllChains startingHeight (pred minMinHeights)
      | otherwise = Nothing
    getRegions = map (\(cid,h) -> OnChain cid minMinHeights (pred h))

data Chunk a =
    AllChains !a !a
  | OnChain !Int64 !a !a

-- data Chunk f a =
--   AllChains (f a)
--   | OnChain !Int64 !Int64 !Int64 (Maybe (f a))

-- fillRegions :: Int64 -> [(Int64,Int64)] -> [Chunk Identity (Int64, Int64)]
-- fillRegions startingHeight minHeights = maybe id ((:)) largestRegion $ getRegions (drop 1 sortedHeights)
--   where
--     minMinHeights = snd $ head sortedHeights
--     sortedHeights = sortOn snd minHeights
--     largestRegion
--       | startingHeight /= minMinHeights = Just $ AllChains $ Identity (startingHeight , pred minMinHeights)
--       | otherwise = Nothing
--     getRegions = map (\(cid,h) -> OnChain cid minMinHeights (pred h) Nothing)

-- limitsAndOffsets' :: Int64 -> Chunk Identity (Int64, Int64) -> Chunk [] (Int64, Int64)
-- limitsAndOffsets' chunkSize = \case
--     AllChains (Identity (low,high)) -> AllChains $ limitsAndOffsets chunkSize low high
--     OnChain cid l h Nothing -> OnChain cid l h (pure $ limitsAndOffsets chunkSize l h)
--     _ -> error "limitsAndOffsets': impossible"
-- -- limitsAndOffsets' chunkSize (AllChains l h) = map (uncurry AllChains) $ limitsAndOffsets chunkSize l h
-- -- limitsAndOffsets' chunkSize (OnChain cid (l,h) _) = map (OnChain cid (l,h) . Just) $ limitsAndOffsets chunkSize l h

-- limitsAndOffsets :: (Ord a, Num a) => a -> a -> a -> [(a,a)]
-- limitsAndOffsets chunkSize l h = go l h []
--   where
--     go low high
--       | chunkSize <= 0 = id
--       | low < high && low + chunkSize <= high = (go (low + chunkSize) high) .  ((chunkSize, low) :)
--       | low < high && low + chunkSize > high = (go (low + chunkSize) high) . ((high-low,low) :)
--       | otherwise = id

dropIndexes :: P.Pool Connection -> [String] -> IO ()
dropIndexes pool indexnames = forM_ indexnames $ \indexname -> P.withResource pool $ \conn ->
  execute_ conn $ Query $ fromString $ printf "ALTER TABLE transfers DROP CONSTRAINT %s;" indexname

withDroppedIndexes :: P.Pool Connection -> IO a -> IO a
withDroppedIndexes pool action = do
    indexNames <- listTranferIndexes pool
    dropIndexes pool indexNames
    action

listTranferIndexes :: P.Pool Connection -> IO [String]
listTranferIndexes pool = P.withResource pool $ \c -> mapMaybe f <$> query_ c qry
  where
    qry = "SELECT tablename,indexname FROM pg_indexes WHERE schemaname='public';"
    f :: (String,String) -> Maybe String
    f (tblname,indexname)
      | tblname == "transfers" = Just indexname
      | otherwise = Nothing

chainMinHeights :: Pg [(Int64, Maybe Int64)]
chainMinHeights = runSelectReturningList $ select $ aggregate_ (\t -> (group_ (_tr_chainid t), min_ (_tr_height t))) (all_ (_cddb_transfers database))

_chainMaxHeights :: Pg [(Int64, Maybe Int64)]
_chainMaxHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_tr_chainid e), max_ (_tr_height e))) (all_ (_cddb_transfers database))

groupEvByChain :: [Event] -> Map Int64 [Event]
groupEvByChain = M.fromListWith (++) . map (\ev -> (_ev_chainid ev, pure ev))

createTransfers :: [Event] -> [UTCTime] -> [Transfer]
createTransfers = zipWith go
  where
    amount ev = case _ev_params ev ^? to unwrap . ix 2 . key "decimal" of
      Just (A.Number n) -> toRealFloat n
      _ -> case _ev_params ev ^? to unwrap . ix 2 . key "int" of
        Just (A.Number n) -> toRealFloat n
        _ -> maybe (error $ msg "amount") toRealFloat $ _ev_params ev ^? to unwrap . ix 2 . _Number
    msg :: String -> String
    msg = printf "Chainweb.BackfillTransfers.createTrantsfer: This entry (%s) was not found"
    from_acct ev = fromMaybe (error $ msg "from_acct") $ _ev_params ev ^? to unwrap . ix 0 . _String
    to_acct ev = fromMaybe (error $ msg "to_acct") $ _ev_params ev ^? to unwrap . ix 1 . _String
    unwrap (PgJSONB a) = a
    go ev creationTime = Transfer
      {
        _tr_creationtime = creationTime
      , _tr_block = _ev_block ev
      , _tr_requestkey = _ev_requestkey ev
      , _tr_chainid = _ev_chainid ev
      , _tr_height = _ev_height ev
      , _tr_idx = _ev_idx ev
      , _tr_qualName = _ev_qualName ev
      , _tr_modulename = _ev_module ev
      , _tr_moduleHash = _ev_moduleHash ev
      , _tr_from_acct = from_acct ev
      , _tr_to_acct = to_acct ev
      , _tr_amount = amount ev
      }

splitEvents :: [Event] -> ([Event],[Event])
splitEvents = partitionEithers . map split
  where
    split ev = case _ev_requestkey ev of
      RKCB_RequestKey _ -> Left ev
      RKCB_Coinbase -> Right ev

-- including the bounding heights triggers index usage (specifically the transactions_height_idx index)
creationTimesByRequestKeysNonCoinbase :: Int64 -> Int64 -> Int64 -> [Event] -> Q Postgres ChainwebDataDb s (C (QExpr Postgres s) UTCTime)
creationTimesByRequestKeysNonCoinbase minHeight maxHeight cid evs = do
    t <- all_ (_cddb_transactions database)
    guard_ $ _tx_chainId t ==. val_ cid
    guard_ $ _tx_height t <. val_ maxHeight
    guard_ $ _tx_height t >=. val_ minHeight
    guard_ $ _tx_requestKey t `in_` (val_ <$> (getDbHashes $ map _ev_requestkey evs))
    return $ _tx_creationTime t
  where
    getDbHashes = mapMaybe $ \case
      RKCB_RequestKey t -> Just t
      _ -> Nothing

creationTimesByRequestKeysCoinbase :: Int64 -> Int64 -> Int64 -> [Event] -> Q Postgres ChainwebDataDb s (C (QExpr Postgres s) UTCTime)
creationTimesByRequestKeysCoinbase minHeight maxHeight cid evs = do
    t <- all_ (_cddb_transactions database)
    guard_ $ _tx_height t <. val_ maxHeight
    guard_ $ _tx_height t >=. val_ minHeight
    guard_ $ _tx_chainId t ==. val_ cid
    guard_ $ _tx_block t `in_` (val_ <$> (getBlockHashes $ map (\ev -> (_ev_block ev, _ev_requestkey ev)) evs))
    return $ _tx_creationTime t
  where
    getBlockHashes  = mapMaybe $ \case
      (bhash, RKCB_Coinbase) -> Just bhash
      _ -> Nothing

eventSelector :: Int64 -> Int64 -> Maybe Int64 -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector startingHeight endingHeight chainid = do
      ev <- all_ (_cddb_events database)
      guard_ $ _ev_height ev <. val_ endingHeight
      guard_ $ _ev_height ev >=. val_ startingHeight
      guard_ $ _ev_name ev ==. val_ "TRANSFER"
      maybe (pure ()) (\c -> guard_ $ _ev_chainid ev ==. val_ c) chainid
      guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 0) ==. (val_ "string")
      guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 1) ==. (val_ "string")
      guard_ $
        pgJsonTypeOf (_ev_params ev -># val_ 2) ==. (val_ "number")
        ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. (val_ "number")
        ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. (val_ "number")
      return ev
  where

eventSelector' :: Int64 -> Int64 -> Maybe Int64 -> Integer -> Integer -> Q Postgres ChainwebDataDb s (EventT (QExpr Postgres s))
eventSelector' startingHeight endingHeight chainid limit offset = limit_ limit $ offset_ offset $ orderBy_ getOrder $ eventSelector startingHeight endingHeight chainid
  where
    getOrder ev = (desc_ $ _ev_height ev, asc_ $ _ev_idx ev)
