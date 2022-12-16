{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleContexts #-}
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

type P = QGenExpr QValueContext Postgres

type N1 s = QNested s
type N2 s = QNested (N1 s)
type N3 s = QNested (N2 s)
type N4 s = QNested (N3 s)

boundedScanOffset :: forall s a db rowT cursorT.
  (SqlOrderable Postgres a, Beamable rowT, Beamable cursorT) =>
  (rowT (P (N3 s)) -> P (N3 s) Bool) ->
  Q Postgres db (N3 s) (rowT (P (N3 s))) ->
  (rowT (P (N3 s)) -> a) ->
  (rowT (P (N3 s)) -> cursorT (P (N3 s))) ->
  Offset ->
  Int64 ->
  Q Postgres db s (cursorT (P s), P s Int64, P s Int64)
boundedScanOffset condExp toScan order toCursor (Offset o) scanLimit =
  limit_ 1 $ do
    (cursor, matchingRow, scan_num, found_num) <- subselect_ $ withWindow_
      (\row ->
        ( frame_ (noPartition_ @Int) (Just $ order row) noBounds_
        , frame_ (noPartition_ @Int) (Just $ order row) (fromBound_ unbounded_)
        )
      )
      (\row (wNoBounds, wTrailing) ->
        ( toCursor row
        , condExp row
        , rowNumber_ `over_` wNoBounds
        , countAll_ `filterWhere_` condExp row `over_` wTrailing
        )
      )
      toScan
    guard_ $ scan_num ==. val_ scanLimit
         ||. (matchingRow &&. found_num ==. val_ (fromInteger o))
    return (cursor, found_num, scan_num)

boundedScanLimit ::
  (SqlOrderable Postgres a, Beamable rowT) =>
  (rowT (P (N1 s)) -> (P (N1 s)) Bool) ->
  Q Postgres db (N4 s) (rowT (P (N4 s))) ->
  (rowT (P (N4 s)) -> a) ->
  Limit ->
  Int64 ->
  Q Postgres db s (rowT (P s), P s Int64, P s Bool)
boundedScanLimit cond toScan order (Limit l) scanLimit = limit_ l $ do
  (row, scan_num) <- subselect_ $ limit_ (fromIntegral scanLimit) $ withWindow_
    (\row -> frame_ (noPartition_ @Int) (Just $ order row) noBounds_)
    (\row window ->
      ( row
      , rowNumber_ `over_` window
      )
    )
    toScan
  let scan_end = scan_num ==. val_ scanLimit
      matchingRow = cond row
  guard_ $ scan_end ||. matchingRow
  return (row, scan_num, matchingRow)

data BSStart newQuery cursor
  = BSNewQuery newQuery
  | BSFromCursor cursor

data BSContinuation cursor = BSContinuation
  { bscCursor :: cursor
  , bscOffset :: Maybe Offset
  }
  deriving (Functor, Foldable, Traversable)

data BoundedScan newQuery cursor row m = BoundedScan
  { bsScanLimit :: Int64
  , bsRunOffset ::
      BSStart newQuery cursor ->
      Offset ->
      Int64 ->
      m (Maybe (cursor, Int64, Int64))
  , bsRunLimit ::
      BSStart newQuery cursor ->
      Limit ->
      Int64 ->
      m [(row,Int64, Bool)]
  , bsOffset :: Maybe Integer
  , bsLimit :: Integer
  , bsStart :: BSStart newQuery cursor
  , bsRowToCursor :: row -> cursor
  }

performBoundedScan ::
  Monad m =>
  BoundedScan newQuery cursor row m ->
  m (Maybe (BSContinuation cursor), [row])
performBoundedScan BoundedScan{..} = do
  let
    runOffset offset = do
      mbCursor <- bsRunOffset bsStart (Offset offset) bsScanLimit
      case mbCursor of
        Nothing -> return (Nothing, [])
        Just (cursor, fromIntegral -> found_cnt, scan_cnt) -> if found_cnt < offset
          then do
            let remainingOffset = Offset $ offset - found_cnt
                cont = BSContinuation cursor $ Just remainingOffset
            return (Just cont, [])
          else runLimit (BSFromCursor cursor) (bsScanLimit - scan_cnt)
    runLimit start toScan = do
      r <- bsRunLimit start (Limit bsLimit) toScan
      let
        scanned = fromMaybe 0 $ lastMay r <&> \(_,scanNum,_) -> scanNum
        mbCursor = case start of
          BSFromCursor cur -> Just cur
          _ -> Nothing
        mbNextCursor = (lastMay r <&> \(ev,_,_) -> bsRowToCursor ev) <|> mbCursor
        mbNextToken = mbNextCursor <&> \cur -> BSContinuation cur Nothing
        results = r <&> \(ev,_,_) -> ev
      return $ if scanned < toScan && fromIntegral (length r) < bsLimit
          then (Nothing, results)
          else (mbNextToken, results)
  case bsOffset of
    Just offset -> runOffset offset
    Nothing -> runLimit bsStart bsScanLimit
