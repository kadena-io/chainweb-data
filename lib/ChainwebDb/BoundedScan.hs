{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module ChainwebDb.BoundedScan (
  boundedScanOffset,
  boundedScanLimit,
  BSStart(..),
  BSContinuation(..),
  BoundedScanParams(..),
  BoundedScan(..),
  performBoundedScan,
) where

import Control.Applicative

import           Data.Int
import           Data.Functor
import           Data.Maybe

import           Database.Beam
import           Database.Beam.Query.Internal (QNested)
import           Database.Beam.Postgres

import           Safe

import           ChainwebData.Pagination

data BoundedScan rowT cursorT s =
  forall ordering.
  SqlOrderable Postgres ordering =>
  BoundedScan
    { bsCondition :: rowT (P s) -> P s Bool
    , bsToCursor :: forall f. rowT f -> cursorT f
    , bsOrdering :: rowT (P s) -> ordering
    }

bsToOffsetQuery :: forall sOut db rowT cursorT.
  (Beamable rowT, Beamable cursorT) =>
  (forall sIn. BoundedScan rowT cursorT sIn) ->
  Q Postgres db (N3 sOut) (rowT (P (N3 sOut))) ->
  Offset ->
  Int64 ->
  Q Postgres db sOut (cursorT (P sOut), P sOut Int64, P sOut Int64)
bsToOffsetQuery bs source = case bs of
  BoundedScan{bsOrdering} -> boundedScanOffset
    (bsCondition bs) source bsOrdering (bsToCursor bs)

bsToLimitQuery :: forall sOut db rowT cursorT.
  (Beamable rowT) =>
  (forall sIn. BoundedScan rowT cursorT sIn) ->
  Q Postgres db (N4 sOut) (rowT (P (N4 sOut))) ->
  Limit ->
  Int64 ->
  Q Postgres db sOut (rowT (P sOut), P sOut Int64, P sOut Bool)
bsToLimitQuery bs source = case bs of
  BoundedScan{bsOrdering} -> boundedScanLimit
    (bsCondition bs) source bsOrdering

boundedScanOffset :: forall s ordering db rowT cursorT.
  (SqlOrderable Postgres ordering, Beamable rowT, Beamable cursorT) =>
  (rowT (P (N3 s)) -> P (N3 s) Bool) ->
  Q Postgres db (N3 s) (rowT (P (N3 s))) ->
  (rowT (P (N3 s)) -> ordering) ->
  (rowT (P (N3 s)) -> cursorT (P (N3 s))) ->
  Offset ->
  Int64 ->
  Q Postgres db s (cursorT (P s), P s Int64, P s Int64)
boundedScanOffset condition source order toCursor (Offset o) scanLimit =
  limit_ 1 $ do
    (cursor, matchingRow, scan_num, found_num) <- subselect_ $ withWindow_
      (\row ->
        ( frame_ (noPartition_ @Int) (Just $ order row) noBounds_
        , frame_ (noPartition_ @Int) (Just $ order row) (fromBound_ unbounded_)
        )
      )
      (\row (wNoBounds, wTrailing) ->
        ( toCursor row
        , condition row
        , rowNumber_ `over_` wNoBounds
        , countAll_ `filterWhere_` condition row `over_` wTrailing
        )
      )
      source
    guard_ $ scan_num ==. val_ scanLimit
         ||. (matchingRow &&. found_num ==. val_ (fromInteger o))
    return (cursor, found_num, scan_num)

boundedScanLimit ::
  (SqlOrderable Postgres ordering, Beamable rowT) =>
  (rowT (P (N1 s)) -> (P (N1 s)) Bool) ->
  Q Postgres db (N4 s) (rowT (P (N4 s))) ->
  (rowT (P (N4 s)) -> ordering) ->
  Limit ->
  Int64 ->
  Q Postgres db s (rowT (P s), P s Int64, P s Bool)
boundedScanLimit cond source order (Limit l) scanLimit = limit_ l $ do
  (row, scan_num) <- subselect_ $ limit_ (fromIntegral scanLimit) $ withWindow_
    (\row -> frame_ (noPartition_ @Int) (Just $ order row) noBounds_)
    (\row window ->
      ( row
      , rowNumber_ `over_` window
      )
    )
    source
  let scan_end = scan_num ==. val_ scanLimit
      matchingRow = cond row
  guard_ $ scan_end ||. matchingRow
  return (row, scan_num, matchingRow)

data BoundedScanParams = BoundedScanParams
  { bspOffset :: Maybe Offset
  , bspResultLimit :: Limit
  , bspScanLimit :: Int64
  }

data BSStart newQuery cursor
  = BSNewQuery newQuery
  | BSFromCursor cursor

data BSContinuation cursor = BSContinuation
  { bscCursor :: cursor
  , bscOffset :: Maybe Offset
  }
  deriving (Functor, Foldable, Traversable)

performBoundedScan :: forall db rowT cursorT newQuery m.
  (FromBackendRow Postgres (rowT Identity), Beamable rowT) =>
  (FromBackendRow Postgres (cursorT Identity), Beamable cursorT) =>
  Monad m =>
  (forall a. Pg a -> m a) ->
  (forall s. BoundedScan rowT cursorT s) ->
  (forall s. BSStart newQuery (cursorT Identity) -> Q Postgres db s (rowT (P s))) ->
  BSStart newQuery (cursorT Identity) ->
  BoundedScanParams ->
  m (Maybe (BSContinuation (cursorT Identity)), [rowT Identity])
performBoundedScan runPg bs source bsStart BoundedScanParams{..} = do
  let
    runOffset offset = do
      mbCursor <- runPg $ runSelectReturningOne $ select $
        bsToOffsetQuery bs (source bsStart) (Offset offset) bspScanLimit
      case mbCursor of
        Nothing -> return (Nothing, [])
        Just (cursor, fromIntegral -> found_cnt, scan_cnt) -> if found_cnt < offset
          then do
            let remainingOffset = Offset $ offset - found_cnt
                cont = BSContinuation cursor $ Just remainingOffset
            return (Just cont, [])
          else runLimit (BSFromCursor cursor) (bspScanLimit - scan_cnt)
    runLimit start scanLim = do
      rows <- runPg $ runSelectReturningList $ select $
        bsToLimitQuery bs (source start) bspResultLimit scanLim
      let
        scanned = fromMaybe 0 $ lastMay rows <&> \(_,scanNum,_) -> scanNum
        mbCursor = case start of
          BSFromCursor cur -> Just cur
          _ -> Nothing
        mbNextCursor = (lastMay rows <&> \(row,_,_) -> bsToCursor bs row) <|> mbCursor
        mbContinuation = mbNextCursor <&> \cur -> BSContinuation cur Nothing
        results = [row | (row,_,found) <- rows, found ]
      return $ if scanned < scanLim && fromIntegral (length rows) < unLimit bspResultLimit
          then (Nothing, results)
          else (mbContinuation, results)
  case bspOffset of
    Just (Offset offset) -> runOffset offset
    Nothing -> runLimit bsStart bspScanLimit

type P = QGenExpr QValueContext Postgres

type N1 s = QNested s
type N2 s = QNested (N1 s)
type N3 s = QNested (N2 s)
type N4 s = QNested (N3 s)
