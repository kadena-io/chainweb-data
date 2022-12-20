{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
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
  Directional, asc, desc,
  cursorCmp,
  CompPair(..), tupleCmp,
) where

import Control.Applicative

import           Data.Coerce (coerce)
import           Data.Functor
import           Data.Functor.Identity(Identity(..))
import           Data.Maybe
import           Data.Proxy (Proxy(..))

import           Database.Beam
import           Database.Beam.Backend
import           Database.Beam.Query.Internal (QNested, QOrd)
import           Database.Beam.Schema.Tables (Beamable(..), Columnar' (..), Ignored)
import           Database.Beam.Postgres

import           Safe

-- | The BoundedScan type represents a search through a rowT relation for rows
-- that satisfy bsCondition returning them in the order defined by bsOrdering
--
-- Semantically, a BoundedScan can be interpreted to represent the following
-- SQL query template:
--
-- > SELECT *
-- > FROM <some-source-relation-to-be-specified-separately> AS r
-- > WHERE $(bsCondition r.*)
-- > ORDER BY $(bsOrdering r.*)
--
-- However, the purpose of the BoundedScan is to be able to execute this query
-- in a way that allows us to specify how many rows we'd like the database to
-- scan before yielding found results and a cursor of type cursorT that we can
-- use to resume the search efficiently later on. A 'BoundedScan' can be
-- passed into 'performBoundedScan' for executing it in this way.
data BoundedScan rowT cursorT = BoundedScan
  { bsCondition :: forall s. rowT (Exp s) -> Exp s Bool
  , bsToCursor :: forall f. rowT f -> cursorT (Directional f)
  }

bsToCursorField :: Beamable cursorT =>
  BoundedScan rowT cursorT -> rowT f -> cursorT f
bsToCursorField bs = unDirectional . bsToCursor bs

asc, desc :: Columnar f a -> Columnar (Directional f) a
asc = Directional Asc . Columnar'
desc = Directional Desc . Columnar'

type Offset = Integer
type ResultLimit = Integer
type ScanLimit = Integer

bsToOffsetQuery :: forall sOut db rowT cursorT.
  (Beamable rowT, Beamable cursorT) =>
  BoundedScan rowT cursorT ->
  QPg db (N3 sOut) (rowT (Exp (N3 sOut))) ->
  Offset ->
  ScanLimit ->
  QPg db sOut (cursorT (Exp sOut), Exp sOut ResultLimit, Exp sOut ScanLimit)
bsToOffsetQuery bs source = boundedScanOffset
  (bsCondition bs) source (bsCursorOrd bs) (bsToCursorField bs)

bsToLimitQuery :: forall sOut db rowT cursorT.
  (Beamable rowT, Beamable cursorT) =>
  BoundedScan rowT cursorT ->
  QPg db (N4 sOut) (rowT (Exp (N4 sOut))) ->
  ResultLimit ->
  ScanLimit ->
  QPg db sOut (rowT (Exp sOut), Exp sOut ScanLimit, Exp sOut Bool)
bsToLimitQuery bs source =
  boundedScanLimit (bsCondition bs) source (bsCursorOrd bs)

bsToUnbounded ::
  (Beamable rowT, Beamable cursorT) =>
  BoundedScan rowT cursorT ->
  QPg db (N3 s) (rowT (Exp (N3 s))) ->
  ResultLimit ->
  Offset ->
  QPg db s (rowT (Exp s))
bsToUnbounded bs source limit offset =
  limit_ limit $ offset_ offset $ orderBy_ (bsCursorOrd bs) $ do
    row <- source
    guard_ $ bsCondition bs row
    return row

-- | Build the offset segment of a 'BoundedScan' query.
--
-- This function will produce a SQL query with the following shape:
--
-- > SELECT <cursor-columns>, found_num, scan_num
-- > FROM (
-- >   SELECT $(toCursor row.*)
-- >        , $(condition row.*) AS matching_row
-- >        , COUNT(*) OVER (ORDER BY $(order row.*)) AS scan_num
-- >        , COUNT(*) FILTER (WHERE $(condition row.*))
-- >            OVER (ORDER BY $(order row.*)) AS found_num
-- >   FROM $source AS row
-- > ) AS t
-- > WHERE scan_num = $scanLimit
-- >    OR (matching_row AND found_num = $offset)
-- > LIMIT 1
--
-- This query will either find the `offset`th occurrence of `condition` and
-- yield a `cursor` that points at it (so that the `LIMIT` segment of the query
-- can start), or it will stop after scanning `scanLimit` number of rows from
-- `source` and yielding the cursor to the row that it stopped.
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

-- | Build the limit segment of a 'BoundedScan' query
--
-- This function will produce a SQL query with the following shape:
--
-- > SELECT <row-columns>, scan_num, matching_row
-- > FROM (
-- >   SELECT row.*, COUNT(*) OVER (ORDER BY $(order row.*))
-- >   FROM $source AS row
-- > ) AS t
-- > WHERE scan_num = $scanLimit OR $(condition t.<row-columns>)
-- > LIMIT $limit
--
-- This query will return up to `limit` number of rows matching `condition`
-- from the `source` table ordered by `order`, but it won't scan any more than
-- `scanLimit` number of rows. If it hits the `scanLimit`th row, the last row
-- returned will have `matchingRow = FALSE` so that it can be discarded from
-- the result set but still used as a cursor to resume the search later.
boundedScanLimit ::
  (SqlOrderable Postgres ordering, Beamable rowT) =>
  (rowT (Exp (N1 s)) -> (Exp (N1 s)) Bool) ->
  QPg db (N4 s) (rowT (Exp (N4 s))) ->
  (rowT (Exp (N4 s)) -> ordering) ->
  ResultLimit ->
  ScanLimit ->
  QPg db s (rowT (Exp s), Exp s ScanLimit, Exp s Bool)
boundedScanLimit condition source order limit scanLimit = limit_ limit $ do
  (row, scan_num) <- subselect_ $ limit_ scanLimit $ withWindow_
    (\row -> frame_ (noPartition_ @Int) (Just $ order row) noBounds_)
    (\row window ->
      ( row
      , rowNumber_ `over_` window
      )
    )
    source
  let scan_end = scan_num ==. val_ scanLimit
      matchingRow = condition row
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

-- | Execute a 'BoundedScan' over a relation of `rowT` rows using a bounded
-- of unbounded scanning strategy.
--
-- Depending on the provided 'ExecutionStrategy', 'performBoundedScan' will
-- either use 'boundedScanOffset' and 'boundedScanLimit' to search through
-- `source`, or it will do a naive `OFFSET ... LIMIT ...` search.
--
-- In both cases, if there are potentially more results to find in subsequent
-- searches, 'performBoundedScan' will return (along with any results found so
-- far) a contiuation that can be used to resume the search efficiently.
performBoundedScan :: forall db rowT cursorT newQuery m.
  -- We want the entire FromBackendRow instance for the limit query to be
  -- assembled at the call site for more optimization opportunities, that
  -- instance is used for parsing every single row after all
  FromBackendRow Postgres (rowT Identity, ScanLimit, Bool) =>
  FromBackendRow Postgres (rowT Identity) =>
  Beamable rowT =>
  (FromBackendRow Postgres (cursorT Identity), Beamable cursorT) =>
  Monad m =>
  ExecutionStrategy ->
  (forall a. Pg a -> m a) ->
  BoundedScan rowT cursorT ->
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
        mbNextCursor = (lastMay rows <&> \(row,_,_) -> bsToCursorField bs row) <|> mbCursor
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
               Just row -> (Just $ BSContinuation (bsToCursorField bs row) Nothing, rows)
  case stg of
    Bounded scanLimit -> case bspOffset of
      Just offset -> runOffset offset scanLimit
      Nothing -> runLimit bsStart scanLimit
    Unbounded -> runUnbounded

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
directionalOrd t = fst $ zipBeamFieldsM mkOrd t tblSkeleton where
  mkOrd ::
    Columnar' (Directional (QExpr backend s)) a ->
    Columnar' Ignored a ->
    ([AnyOrd backend s], Columnar' Proxy a)
  mkOrd (Columnar' (Directional dir (Columnar' q))) _ = ([ord],Columnar' Proxy)
    where ord = anyOrd $ case dir of
            Asc -> asc_ q
            Desc -> asc_ q

-- | This function stripts the type of the field in a QOrd so that they can be
-- gathered in a list
anyOrd :: QOrd be s a -> AnyOrd be s
anyOrd = coerce

type AnyOrd be s = QOrd be s ()

unDirectional :: Beamable t =>
  t (Directional f) -> t f
unDirectional t = runIdentity $ zipBeamFieldsM mkOrd t tblSkeleton where
  mkOrd (Columnar' (Directional _ (Columnar' q))) _ = Identity $ Columnar' q

bsCursorOrd :: (BeamSqlBackend backend, Beamable cursorT) =>
  BoundedScan rowT cursorT -> rowT (QExpr backend s) -> [AnyOrd backend s]
bsCursorOrd bs = directionalOrd . bsToCursor bs

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
-- Some internal utility definitions used to shorten the types in this module

type QPg = Q Postgres

type Exp = QGenExpr QValueContext Postgres

type N1 s = QNested s
type N2 s = QNested (N1 s)
type N3 s = QNested (N2 s)
type N4 s = QNested (N3 s)
