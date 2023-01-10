{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module ChainwebDb.BoundedScan (
  FilterMarked(..),
  applyFilterMark,
  BSContinuation(..),
  ExecutionStrategy(..),
  performBoundedScan,
  boundedScanOffset,
  boundedScanLimit,
  Directional, asc, desc,
  cursorCmp,
  CompPair(..), tupleCmp,
  noDecoration,
) where

import Control.Applicative

import           Data.Coerce (coerce)
import           Data.Functor
import           Data.Functor.Identity(Identity(..))
import           Data.Kind (Type)
import           Data.Maybe
import           Data.Proxy (Proxy(..))

import           Database.Beam
import           Database.Beam.Backend
import           Database.Beam.Query.Internal (QOrd)
import           Database.Beam.Schema.Tables (Beamable(..), Columnar' (..))
import           Database.Beam.Postgres

import           Safe

asc, desc :: Columnar f a -> Columnar (Directional f) a
asc = Directional Asc . Columnar'
desc = Directional Desc . Columnar'

data FilterMarked rowT f = FilterMarked
  { fmMatch :: C f Bool
  , fmRow :: rowT f
  } deriving (Generic, Beamable)

applyFilterMark ::
  QPg db s (FilterMarked rowT (Exp s)) ->
  QPg db s (rowT (Exp s))
applyFilterMark source = do
  row <- source
  guard_ $ fmMatch row
  return $ fmRow row

type Offset = Integer
type ResultLimit = Integer
type ScanLimit = Integer

-- | Build the offset segment of a bounded scan query.
--
-- This function will produce a SQL query with the following shape:
--
-- > SELECT <cursor-columns>, found_num, scan_num
-- > FROM (
-- >   SELECT $(toCursor rest)
-- >        , COUNT(*) OVER (ORDER BY $(orderOverCursor rest)) AS scan_num
-- >        , COUNT(*) FILTER (WHERE match)
-- >            OVER (ORDER BY $(orderOverCursor rest)) AS found_num
-- >   FROM $source AS t(match,rest...)
-- >   ORDER BY $(orderOverCursor rest)
-- >   LIMIT $scanLimit
-- > ) AS t
-- > WHERE scan_num = $scanLimit
-- >    OR found_num = $offset
-- > ORDER BY $(orderOverCursor <cursor-columns>)
-- > LIMIT 1
--
-- This query will either find the `offset`th row of `source` with `match = TRUE` and
-- yield a `cursor` that points at it (so that the `LIMIT` segment of the query
-- can start), or it will stop after scanning `scanLimit` number of rows from
-- `source` and yielding the cursor to the row that it stopped.
--
-- Note that this function expects the passed in `offset` to be greater than zero
-- since there's no meaningful cursor it could return in the offset=0 case.
boundedScanOffset :: forall s db rowT cursorT.
  (Beamable rowT, Beamable cursorT) =>
  (forall s2. QPg db s2 (FilterMarked rowT (Exp s2))) ->
  (forall f. rowT f -> cursorT (Directional f)) ->
  Offset ->
  ScanLimit ->
  QPg db s (cursorT (Exp s), (Exp s ResultLimit, Exp s ScanLimit))
boundedScanOffset source toCursor offset scanLimit =
  limit_ 1 $ orderBy_ (\(cur,_) -> orderCursor cur) $ do
    (cursor, scan_num, found_num) <-
      subselect_ $ limit_ scanLimit $ orderBy_ (\(cur,_,_) -> orderCursor cur) $ withWindow_
        (\row ->
          ( frame_ (noPartition_ @Int) (Just $ orderRow $ fmRow row) noBounds_
          , frame_ (noPartition_ @Int) (Just $ orderRow $ fmRow row) (fromBound_ unbounded_)
          )
        )
        (\row (wNoBounds, wTrailing) ->
          ( unDirectional $ toCursor $ fmRow row
          , rowNumber_ `over_` wNoBounds
          , countAll_ `filterWhere_` fmMatch row `over_` wTrailing
          )
        )
        source
    guard_ $ scan_num ==. val_ scanLimit
         ||. found_num ==. val_ offset
    return (cursor, (found_num, scan_num))
  where orderRow = directionalOrd . toCursor
        orderCursor :: forall s3. cursorT (Exp s3) -> [AnyOrd Postgres s3]
        orderCursor cur = directionalOrdZip (toCursor tblSkeleton) cur


-- | Build the limit segment of a bounded scan query
--
-- This function will produce a SQL query with the following shape:
--
-- > SELECT rest, scan_num, matching_row
-- > FROM (
-- >   SELECT t.*, COUNT(*) OVER (ORDER BY $(orderOverCursor rest))
-- >   FROM $source AS t(match,rest...)
-- > ) AS t
-- > WHERE scan_num = $scanLimit OR match
--
-- This query will return up to `limit` number of rows of the `source` table
-- with `match = True` ordered by `order`, but it won't scan any more than
-- `scanLimit` number of rows. If it hits the `scanLimit`th row, the last row
-- returned will have `matchingRow = FALSE` so that it can be discarded from
-- the result set but still used as a cursor to resume the search later.
boundedScanLimit ::
  (Beamable rowT) =>
  (forall s2. QPg db s2 (FilterMarked rowT (Exp s2))) ->
  (forall s2. rowT (Exp s2) -> [AnyOrd Postgres s2]) ->
  ScanLimit ->
  QPg db s (rowT (Exp s), (Exp s ScanLimit, Exp s Bool))
boundedScanLimit source order scanLimit = do
  (row, scan_num) <- subselect_ $ limit_ scanLimit $ withWindow_
    (\row -> frame_ (noPartition_ @Int) (Just $ order $ fmRow row) noBounds_)
    (\row window ->
      ( row
      , rowNumber_ `over_` window
      )
    )
    source
  let scan_end = scan_num ==. val_ scanLimit
      matchingRow = fmMatch row
  guard_ $ scan_end ||. matchingRow
  return (fmRow row, (scan_num, matchingRow))

data BSContinuation cursor = BSContinuation
  { bscCursor :: cursor
  , bscOffset :: Maybe Offset
  }
  deriving (Functor, Foldable, Traversable)

data ExecutionStrategy = Bounded ScanLimit | Unbounded

-- | Execute a bounded scan over a relation of `rowT` rows using a bounded
-- or unbounded scanning strategy.
--
-- Depending on the provided 'ExecutionStrategy', 'performBoundedScan' will
-- either use 'boundedScanOffset' and 'boundedScanLimit' to search through
-- `source`, or it will do a naive `OFFSET ... LIMIT ...` search.
--
-- In both cases, if there are potentially more results to find in subsequent
-- searches, 'performBoundedScan' will return (along with any results found so
-- far) a contiuation that can be used to resume the search efficiently.
performBoundedScan :: forall db rowT extrasT cursorT m.
  FromBackendRow Postgres (rowT Identity, extrasT Identity) =>
  (Beamable rowT, Beamable extrasT) =>
  (FromBackendRow Postgres (cursorT Identity), SqlValableTable Postgres cursorT) =>
  Monad m =>
  ExecutionStrategy ->
  (forall a. Pg a -> m a) ->
  -- | Convert a row to a cursor with order direction annotations
  (forall f. rowT f -> cursorT (Directional f)) ->
  -- | A relation of rows with annotations indicating whether each row should be kept
  (forall s. QPg db s (FilterMarked rowT (Exp s))) ->
  (forall s. rowT (Exp s) -> QPg db s (extrasT (Exp s))) ->
  -- | The start of this execution, indicates whether this is a new query or the
  -- contionation of a previous execution.
  Either (Maybe Offset) (BSContinuation (cursorT Identity)) ->
  -- | The maximum number of rows to return
  ResultLimit ->
  m ( Maybe (BSContinuation (cursorT Identity))
    , [(rowT Identity, extrasT Identity)]
    )
performBoundedScan stg runPg toCursor source decorate contination resultLimit = do
  let
    runOffset mbStart offset scanLimit = do
      mbCursor <- runPg $ runSelectReturningOne $ select $ boundedScanOffset
        (resumeSource toCursor source mbStart)
        toCursor
        offset
        scanLimit
      case mbCursor of
        Nothing -> return (Nothing, [])
        Just (cursor, (found_cnt, scan_cnt)) -> if found_cnt < offset
          then do
            let remainingOffset = offset - found_cnt
                cont = BSContinuation cursor $ Just remainingOffset
            return (Just cont, [])
          else runLimit (Just cursor) (scanLimit - scan_cnt)

    runLimit mbStart scanLim = do
      rows <- runPg $ runSelectReturningList $ select $
        limit_ resultLimit $
        orderBy_ (\((row,_),_) -> directionalOrd $ toCursor row ) $ do
          (row, bsBookKeeping) <- boundedScanLimit
            (resumeSource toCursor source mbStart)
            (directionalOrd . toCursor)
            scanLim
          extras <- decorate row
          return ((row, extras), bsBookKeeping)
      let
        scanned = fromMaybe 0 $ lastMay rows <&> \(_,(scanNum,_)) -> scanNum
        mbNextCursor = (lastMay rows <&> \((row,_),_) -> unDirectional $ toCursor row)
                   <|> mbStart
        mbContinuation = mbNextCursor <&> \cur -> BSContinuation cur Nothing
        results = [row | (row,(_,found)) <- rows, found ]
      return $ if scanned < scanLim && fromIntegral (length rows) < resultLimit
          then (Nothing, results)
          else (mbContinuation, results)

    runUnbounded mbStart mbOffset = do
      let sourceCont = resumeSource toCursor source mbStart
          offset = fromMaybe 0 mbOffset
      rows <- runPg $ runSelectReturningList $ select $
        limit_ resultLimit  $
        offset_ offset $
        orderBy_ (directionalOrd . toCursor . fst) $ do
        row <- applyFilterMark sourceCont
        extras <- decorate row
        return (row, extras)
      return $ (,rows) $ if fromIntegral (length rows) < resultLimit
        then Nothing
        else lastMay rows <&> \row ->
               BSContinuation (unDirectional $ toCursor $ fst row) Nothing

    (mbStartTop, mbOffsetTop) = case contination of
      Left mbO -> (Nothing, mbO)
      Right (BSContinuation cursor mbO) -> (Just cursor, mbO)
  case stg of
    Bounded scanLimit -> case mbOffsetTop of
      Just offset | offset > 0 -> runOffset mbStartTop offset scanLimit
      _ -> runLimit mbStartTop scanLimit
    Unbounded -> runUnbounded mbStartTop mbOffsetTop

resumeSource :: (SqlValableTable Postgres cursorT) =>
  (rowT (Exp s) -> cursorT (Directional (Exp s))) ->
  QPg db s (FilterMarked rowT (Exp s)) ->
  Maybe (cursorT Identity) ->
  QPg db s (FilterMarked rowT (Exp s))
resumeSource toCursor source mbResume = case mbResume of
  Nothing -> source
  Just cursor -> do
    row <- source
    guard_ $ cursorCmp (>.) (toCursor $ fmRow row) cursor
    return row

-------------------------------------------------------------------------------
-- Utilities for constructing SQL expressions comparing two tuples

data CompPair be s = forall t. (:<>) (QExpr be s t ) (QExpr be s t)

tupleCmp
  :: IsSql92ExpressionSyntax (BeamSqlBackendExpressionSyntax be)
  => (forall t. QExpr be s t -> QExpr be s t -> QExpr be s Bool)
  -> [CompPair be s]
  -> QExpr be s Bool
tupleCmp cmp cps = QExpr lExp `cmp` QExpr rExp where
  lExp = rowE <$> sequence [e | QExpr e :<> _ <- cps]
  rExp = rowE <$> sequence [e | _ :<> QExpr e <- cps]

-------------------------------------------------------------------------------
-- Utilities for working on cursors with fields that have direction annotations

-- | A Dir indicates a SQL ORDER BY direction
data Dir = Asc | Desc

-- | (Directional f) is meant to be used as an argument to a 'Beamable' table,
-- annotating each field with a Dir
data Directional f a = Directional Dir (Columnar' f a)

-- | Given a 'Beamable' row containing Dir-annotated SQL expressions, return
-- a value that can be passed to an 'orderBy_' that orders the results in
-- "cursor order", i.e. in ascending cursor values.
directionalOrd :: forall t backend s. (BeamSqlBackend backend, Beamable t) =>
  t (Directional (QExpr backend s)) -> [AnyOrd backend s]
directionalOrd t = directionalOrdZip t $ unDirectional t

-- | Given a 'Beamable' row containing Dir-annotations and another row of the
-- same shape containing SQL expressions, return a value that can be passed to
-- an 'orderBy_' that orders the results in "cursor order", i.e. in ascending
-- cursor values.
directionalOrdZip :: forall t backend s ignored. (BeamSqlBackend backend, Beamable t) =>
  t (Directional ignored) -> t (QExpr backend s) -> [AnyOrd backend s]
directionalOrdZip tDirs tExps = fst $ zipBeamFieldsM mkOrd tDirs tExps where
  mkOrd ::
    Columnar' (Directional ignored) a ->
    Columnar' (QExpr backend s) a ->
    ([AnyOrd backend s], Columnar' Proxy a)
  mkOrd (Columnar' (Directional dir _)) (Columnar' q) = ([ord],Columnar' Proxy)
    where ord = anyOrd $ case dir of
            Asc -> asc_ q
            Desc -> desc_ q

-- | This function stripts the type of the field in a QOrd so that they can be
-- gathered in a list
anyOrd :: QOrd be s a -> AnyOrd be s
anyOrd = coerce

type AnyOrd be s = QOrd be s ()

unDirectional :: Beamable t =>
  t (Directional f) -> t f
unDirectional t = runIdentity $ zipBeamFieldsM mkOrd t tblSkeleton where
  mkOrd (Columnar' (Directional _ (Columnar' q))) _ = Identity $ Columnar' q

-- | Compare a cursor expression with direction annotations against a cursor
-- value in "cursor order". Cursor order means that whichever cursor is ahead
-- as viewed as a moving cursor is considered greater.
cursorCmp :: (SqlValableTable Postgres cursorT) =>
  (forall t. Exp s t -> Exp s t -> Exp s Bool) ->
  cursorT (Directional (Exp s)) ->
  cursorT Identity -> Exp s Bool
cursorCmp cmpOp cursorExp cursorVal = tupleCmp cmpOp cmpPairs where
  cmpPairs = fst $ zipBeamFieldsM mkPair cursorExp (val_ cursorVal)
  mkPair ::
    Columnar' (Directional (Exp s)) a ->
    Columnar' (Exp s) a ->
    ([CompPair Postgres s], Columnar' Proxy a)
  mkPair (Columnar' (Directional dir (Columnar' lExp))) (Columnar' rExp) =
    ([withDirOrder dir lExp rExp],Columnar' Proxy)
  withDirOrder Asc lExp rExp = lExp :<> rExp
  withDirOrder Desc lExp rExp =  rExp :<> lExp

-------------------------------------------------------------------------------
-- | UnitF is a beam record with no fields, so returning it doesn't actually
-- cause new columns to be defined in a SELECT query.
data UnitF (f :: Type -> Type) = UnitF deriving (Generic, Beamable, Show)

-- | noDecoration can be used as the decoration of a performBoundedScan call
-- without causing any overhead on the database side.
noDecoration :: Monad m => a -> m (UnitF (QGenExpr ctx be s))
noDecoration _ = return UnitF

-------------------------------------------------------------------------------
-- Some internal utility definitions used to shorten the types in this module

type QPg = Q Postgres

type Exp = QGenExpr QValueContext Postgres
