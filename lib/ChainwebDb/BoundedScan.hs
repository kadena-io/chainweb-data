{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module ChainwebDb.BoundedScan (
  BoundedScan(..),
  BSStart(..),
  BSContinuation(..),
  BoundedScanParams(..),
  ExecutionStrategy(..),
  performBoundedScan,
  bsToOffsetQuery,
  bsToLimitQuery,
  bsToUnbounded,
  boundedScanOffset,
  boundedScanLimit,
) where

import Control.Applicative

import           Data.Functor
import           Data.Maybe

import           Database.Beam
import           Database.Beam.Query.Internal (QNested)
import           Database.Beam.Postgres

import           Safe

data BoundedScan rowT cursorT s =
  forall ordering. SqlOrderable Postgres ordering =>
  BoundedScan
    { bsCondition :: rowT (Exp s) -> Exp s Bool
    , bsToCursor :: forall f. rowT f -> cursorT f
    , bsOrdering :: rowT (Exp s) -> ordering
    }

type Offset = Integer
type ResultLimit = Integer
type ScanLimit = Integer

bsToOffsetQuery :: forall sOut db rowT cursorT.
  (Beamable rowT, Beamable cursorT) =>
  (forall sIn. BoundedScan rowT cursorT sIn) ->
  QPg db (N3 sOut) (rowT (Exp (N3 sOut))) ->
  Offset ->
  ScanLimit ->
  QPg db sOut (cursorT (Exp sOut), Exp sOut ResultLimit, Exp sOut ScanLimit)
bsToOffsetQuery bs@BoundedScan{bsOrdering} source =
  boundedScanOffset (bsCondition bs) source bsOrdering (bsToCursor bs)

bsToLimitQuery :: forall sOut db rowT cursorT.
  (Beamable rowT) =>
  (forall sIn. BoundedScan rowT cursorT sIn) ->
  QPg db (N4 sOut) (rowT (Exp (N4 sOut))) ->
  ResultLimit ->
  ScanLimit ->
  QPg db sOut (rowT (Exp sOut), Exp sOut ScanLimit, Exp sOut Bool)
bsToLimitQuery bs@BoundedScan{bsOrdering} source =
  boundedScanLimit (bsCondition bs) source bsOrdering

bsToUnbounded ::
  (Beamable rowT) =>
  (forall sIn. BoundedScan rowT cursorT sIn) ->
  QPg db (N3 s) (rowT (Exp (N3 s))) ->
  ResultLimit ->
  Offset ->
  QPg db s (rowT (Exp s))
bsToUnbounded BoundedScan{..} source limit offset =
  limit_ limit $ offset_ offset $ orderBy_ bsOrdering $ do
    row <- source
    guard_ $ bsCondition row
    return row

boundedScanOffset :: forall s ordering db rowT cursorT.
  (SqlOrderable Postgres ordering, Beamable rowT, Beamable cursorT) =>
  (rowT (Exp (N3 s)) -> Exp (N3 s) Bool) ->
  QPg db (N3 s) (rowT (Exp (N3 s))) ->
  (rowT (Exp (N3 s)) -> ordering) ->
  (rowT (Exp (N3 s)) -> cursorT (Exp (N3 s))) ->
  Offset ->
  ScanLimit ->
  QPg db s (cursorT (Exp s), Exp s ResultLimit, Exp s ScanLimit)
boundedScanOffset condition source order toCursor offset scanLimit =
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
         ||. (matchingRow &&. found_num ==. val_ offset)
    return (cursor, found_num, scan_num)

boundedScanLimit ::
  (SqlOrderable Postgres ordering, Beamable rowT) =>
  (rowT (Exp (N1 s)) -> (Exp (N1 s)) Bool) ->
  QPg db (N4 s) (rowT (Exp (N4 s))) ->
  (rowT (Exp (N4 s)) -> ordering) ->
  ResultLimit ->
  ScanLimit ->
  QPg db s (rowT (Exp s), Exp s ScanLimit, Exp s Bool)
boundedScanLimit cond source order limit scanLimit = limit_ limit $ do
  (row, scan_num) <- subselect_ $ limit_ scanLimit $ withWindow_
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
  , bspResultLimit :: ResultLimit
  }

data BSStart newQuery cursor
  = BSNewQuery newQuery
  | BSFromCursor cursor

data BSContinuation cursor = BSContinuation
  { bscCursor :: cursor
  , bscOffset :: Maybe Offset
  }
  deriving (Functor, Foldable, Traversable)

data ExecutionStrategy = Bounded ScanLimit | Unbounded

performBoundedScan :: forall db rowT cursorT newQuery m.
  -- We want the entire FromBackendRow instance for the limit query to be
  -- assembled at the call site for more optimization opportunities, that
  -- instance is used for parsing every single row after all
  FromBackendRow Postgres (rowT Identity, ScanLimit, Bool) =>
  (FromBackendRow Postgres (rowT Identity)) =>
  Beamable rowT =>
  (FromBackendRow Postgres (cursorT Identity), Beamable cursorT) =>
  Monad m =>
  ExecutionStrategy ->
  (forall a. Pg a -> m a) ->
  (forall s. BoundedScan rowT cursorT s) ->
  (forall s. BSStart newQuery (cursorT Identity) -> QPg db s (rowT (Exp s))) ->
  BSStart newQuery (cursorT Identity) ->
  BoundedScanParams ->
  m (Maybe (BSContinuation (cursorT Identity)), [rowT Identity])
performBoundedScan stg runPg bs source bsStart BoundedScanParams{..} = do
  let
    runOffset offset scanLimit = do
      mbCursor <- runPg $ runSelectReturningOne $ select $
        bsToOffsetQuery bs (source bsStart) offset scanLimit
      case mbCursor of
        Nothing -> return (Nothing, [])
        Just (cursor, found_cnt, scan_cnt) -> if found_cnt < offset
          then do
            let remainingOffset = offset - found_cnt
                cont = BSContinuation cursor $ Just remainingOffset
            return (Just cont, [])
          else runLimit (BSFromCursor cursor) (scanLimit - scan_cnt)
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
      return $ if scanned < scanLim && fromIntegral (length rows) < bspResultLimit
          then (Nothing, results)
          else (mbContinuation, results)
    runUnbounded = do
      rows <- runPg $ runSelectReturningList $ select $
        bsToUnbounded bs (source bsStart) bspResultLimit $ fromMaybe 0 bspOffset
      return $ if fromIntegral (length rows) >= bspResultLimit
        then (Nothing,rows)
        else case lastMay rows of
               Nothing -> (Nothing,rows)
               Just row -> (Just $ BSContinuation (bsToCursor bs row) Nothing, rows)
  case stg of
    Bounded scanLimit -> case bspOffset of
      Just offset -> runOffset offset scanLimit
      Nothing -> runLimit bsStart scanLimit
    Unbounded -> runUnbounded

type QPg = Q Postgres

type Exp = QGenExpr QValueContext Postgres

type N1 s = QNested s
type N2 s = QNested (N1 s)
type N3 s = QNested (N2 s)
type N4 s = QNested (N3 s)
