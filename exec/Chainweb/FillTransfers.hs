{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Chainweb.FillTransfers ( fillTransfers ) where

import           BasePrelude hiding (insert, range, second)

import           Chainweb.Api.NodeInfo
import           ChainwebDb.Database
import           ChainwebData.Env
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Transaction

import qualified Data.Pool as P

import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full
import           Database.Beam.Query.DataTypes ()
import           Database.PostgreSQL.Simple.Transaction

import           System.Logger.Types hiding (logg)

---

-- fill an empty transfers table (steps)
-- 1. check if transfers table is actually empty
-- 2. check if events table has any coinbase gaps. If so, tell user to fill those gaps
-- 3. Fill from last known max height on each chain all the way back to events activation height(s)

fillTransfers :: Env -> IO ()
fillTransfers env = do

    transfersNonEmpty <- withDb env $ runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_transfers database) $> as_ @Int32 (val_ 1))
    case transfersNonEmpty of
      Just True -> pure ()
      Just False -> die "ERROR: transfers table does not exist"
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as transfers);) failed somehow."

    eventsTableNonEmpty <- withDb env $ runSelectReturningOne $ select $ pure $ exists_ (all_ (_cddb_events database) $> as_ @Int32 (val_ 1))
    case eventsTableNonEmpty of
      Just True -> pure ()
      Just False -> die "ERROR: events table does not exist"
      Nothing -> die "IMPOSSIBLE: This query (SELECT EXISTS (SELECT 1 as events);) failed somehow."

    let startingHeight = case _nodeInfo_chainwebVer $ _env_nodeInfo env of
          "mainnet01" -> 1722501
          "testnet04" -> 1261001
          _ -> error "Chainweb version: Unknown"
    maxHeights <- withDbDebug env Debug chainMaxHeights
    let errNothing msg = maybe (error msg) id
        heightMsg = printf "fillTransfers: Cannot get height: %s"
    mapM_ (\(cid,h) -> logg Info $ fromString $ printf "Filling transfers table on chain %d from height %d to %d." cid startingHeight (errNothing heightMsg h)) maxHeights
    P.withResource pool $ \c -> withTransaction c $ runBeamPostgres c $
        runInsert
         $ insert (_cddb_transfers database) (insertFrom (eventSelector startingHeight))
         $ onConflict (conflictingFields primaryKey) onConflictDoNothing

  where
    logg = _env_logger env
    pool = _env_dbConnPool env

chainMaxHeights :: Pg [(Int64, Maybe Int64)]
chainMaxHeights = runSelectReturningList $ select $ aggregate_ (\e -> (group_ (_tr_chainid e), max_ (_tr_height e))) (all_ (_cddb_transfers database))

eventSelector :: Int64 -> Q Postgres ChainwebDataDb s (TransferT (QExpr Postgres s))
eventSelector startingHeight = do
    ev <- all_ (_cddb_events database)
    t <- all_ (_cddb_transactions database)
    guard_ $ _ev_height ev >=. val_ startingHeight
    guard_ $ _ev_name ev ==. val_ "TRANSFER"
    guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 0) ==. (val_ "string")
    guard_ $ pgJsonTypeOf (_ev_params ev -># val_ 1) ==. (val_ "string")
    guard_ $
      pgJsonTypeOf (_ev_params ev -># val_ 2) ==. (val_ "number")
      ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. (val_ "number")
      ||. pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. (val_ "number")
    let from_acct = _ev_params ev -># val_ 0
        to_acct = _ev_params ev -># val_ 1
        -- this is ugly, but we have to do things the sql way
        amount = ifThenElse_ (pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "decimal") ==. val_ "number")
                (_ev_params ev -># val_ 2 ->$ "decimal")
                 (ifThenElse_ (pgJsonTypeOf (_ev_params ev -># val_ 2 ->$ "int") ==. val_ "number") (_ev_params ev -># val_ 2 ->$ "int")
                        (_ev_params ev -># val_ 2))
    return Transfer
      { _tr_creationtime = _tx_creationTime t
      , _tr_block = _ev_block ev
      , _tr_requestkey = _ev_requestkey ev
      , _tr_chainid = _ev_chainid ev
      , _tr_height = _ev_height ev
      , _tr_idx = _ev_idx ev
      , _tr_modulename = _ev_module ev
      , _tr_from_acct = cast_ from_acct (varchar Nothing)
      , _tr_to_acct = cast_ to_acct (varchar Nothing)
      , _tr_amount =  cast_ amount double
      }
