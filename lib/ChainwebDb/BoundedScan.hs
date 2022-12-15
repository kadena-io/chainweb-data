{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module ChainwebDb.BoundedScan (
  boundedScanOffset,
  boundedScanLimit,
) where

import           Data.Int

import           Database.Beam
import           Database.Beam.Query.Internal (QNested)
import           Database.Beam.Postgres

import           ChainwebData.Pagination

type PgExpr s = QGenExpr QValueContext Postgres s

type QBS = QBaseScope
type NQBS = QNested QBS
type N2QBS = QNested NQBS
type N3QBS = QNested N2QBS
type N4QBS = QNested N3QBS

boundedScanOffset :: forall a db rowT cursorT.
  (SqlOrderable Postgres a, Beamable rowT, Beamable cursorT) =>
  (forall s. rowT (PgExpr s) -> PgExpr s Bool) ->
  Q Postgres db N3QBS (rowT (PgExpr N3QBS)) ->
--  (forall s. Q Postgres ChainwebDataDb s (EventT (PgExpr s))) ->
  (rowT (PgExpr N3QBS) -> a) ->
  (rowT (PgExpr N3QBS) -> cursorT (PgExpr N3QBS)) ->
  Offset ->
  Int64 ->
  SqlSelect Postgres (cursorT Identity, Int64, Int64)
boundedScanOffset condExp toScan order toCursor (Offset o) scanLimit =
  select $ limit_ 1 noLimitQuery where
  -- For some reason, beam fails to unify the types here unless we move noLimitQuery
  -- to a separate definition and explicitly specify its type
  noLimitQuery :: Q Postgres db NQBS
    ( cursorT (PgExpr NQBS)
    , PgExpr NQBS Int64
    , PgExpr NQBS Int64
    )
  noLimitQuery = do
    (cursor, matchingRow, scan_num, found_num) <- subselect_ $ withWindow_
      (\ev ->
        ( frame_ (noPartition_ @Int) (Just $ order ev) noBounds_
        , frame_ (noPartition_ @Int) (Just $ order ev) (fromBound_ unbounded_)
        )
      )
      (\ev (wNoBounds, wTrailing) ->
        ( toCursor ev
        , condExp ev
        , rowNumber_ `over_` wNoBounds
        , countAll_ `filterWhere_` condExp ev `over_` wTrailing
        )
      )
      toScan
    guard_ $ scan_num ==. val_ scanLimit ||. (matchingRow &&. found_num ==. val_ (fromInteger o))
    return (cursor, found_num, scan_num)

boundedScanLimit ::
  (SqlOrderable Postgres a, Beamable rowT) =>
  (rowT (PgExpr NQBS) -> (PgExpr NQBS) Bool) ->
  Q Postgres db N4QBS (rowT (PgExpr N4QBS)) ->
  (rowT (PgExpr N4QBS) -> a) ->
  Limit ->
  Int64 ->
  Q Postgres db QBS (rowT (PgExpr QBS), PgExpr QBS Int64, PgExpr QBS Bool)
boundedScanLimit cond toScan order (Limit l) scanLimit = limit_ l $ do
  (ev, scan_num) <- subselect_ $ limit_ (fromIntegral scanLimit) $ withWindow_
    (\ev -> frame_ (noPartition_ @Int) (Just $ order ev) noBounds_)
    (\ev window ->
      ( ev
      , rowNumber_ `over_` window
      )
    )
    toScan
  let scan_end = scan_num ==. val_ scanLimit
      matchingRow = cond ev
  guard_ $ scan_end ||. matchingRow
  return (ev, scan_num, matchingRow)
