{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.FillTransfers ( fillTransfers ) where

import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           Chainweb.Lookups
import           Chainweb.Worker
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race_)

import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

---

fillTransfers :: Env -> BackfillArgs -> IO ()
fillTransfers env args = do
  ecut <- queryCut env
  case ecut of
    Left e -> do
      let logg = _env_logger env
      logg Error $ fromString $ printf "Error querying cut"
      logg Error $ fromString $ show e
    Right cutBS -> fillOnlyTransfersCut env args cutBS

-- fill an empty transfers table (steps)
 -- 1. check if transfers table is actually empty
  -- 2. check if events table has any gaps. If so, tell user to fill those gaps
  -- 3. Fill from last known max height on each chain all the way back to events activation height(s)


fillOnlyTransfersCut :: Env -> BackfillArgs -> ByteString -> IO ()
fillOnlyTransfersCut env args cutBS = do
    let curHeight = cutMaxHeight cutBS
        cids = atBlockHeight (fromIntegral curHeight) allCids

    transferTableCount <- withDb env $ runSelectReturningOne $ select $ aggregate_ (const $ as_ @Int32 countAll_) (all_ $ _cddb_transfers database)
    case transferTableCount of
      Just 0 -> die ""
      Just _ -> pure ()
      Nothing -> die "" -- something bad happened

    eventsTableCount <- withDb env $ runSelectReturningOne $ select $ aggregate_ (const $ as_ @Int32 countAll_) (all_ $ _cddb_events database)
    case eventsTableCount of
      Just 0 -> die ""
      Just _ -> pure ()
      Nothing -> die "" -- something bad happened


    maxHeights <- withDbDebug env Debug chainMaxHeights

    minHeights <- withDbDebug env Debug chainMinHeights
    let count = length minHeights
    when (count /= length cids) $ do
        let startingHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
              "mainnet01" -> 1722501
              "testnet04" -> 1261001
              _ -> error "Chainweb version: Unknown"
        gaps :: [(Int64,Int64,Int64)] <- undefined
        mapM_ (logg Debug . fromString . show) gaps
        let numMissingCoinbase = sum $ map (\(_,a,b) -> b - a - 1) gaps
        logg Info $ fromString $ printf "Got %d gaps" (length gaps)

        if null gaps
          then do
              logg Info "There are no missing transfers in any of the chains"
              exitSuccess
          else undefined


  where
    logg = _env_logger env
    delay = _backfillArgs_delayMicros args
    pool = _env_dbConnPool env
    allCids = _env_chainsAtHeight env


-- getTransferGaps :: Env -> Int64 -> [(Int64, Maybe Int64)] -> IO [(Int64, Int64, Int64)]
-- getTransferGaps env startingHeight minHeights = withDbDebug env Debug $ do
--     gaps <- undefined
--     liftIO $ logg Debug $ fromString $ "minHeights: " <> show minHeights
--     let addStart [] = gaps
--         addStart ((_,Nothing):ms) = addStart ms
--         addStart ((cid,Just m):ms) =
--           if m > startingHeight
--           then (cid,startingHeight, m) : addStart ms
--           else addStart ms
--     pure $ addStart minHeights
--   where
--     logg = _env_logger env

chainMinHeights :: Pg [(Int64, Maybe Int64)]
chainMinHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_tr_chainid e), min_ (_tr_height e))) (all_ (_cddb_transfers database))

chainMaxHeights :: Pg [(Int64, Maybe Int64)]
chainMaxHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_tr_chainid e), max_ (_tr_height e))) (all_ (_cddb_transfers database))
