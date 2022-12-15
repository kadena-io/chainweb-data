{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
-- |

module ChainwebDb.Queries where

------------------------------------------------------------------------------
import           Data.Aeson hiding (Error)
import           Data.ByteString.Lazy (ByteString)
import           Data.Int
import           Data.Maybe (maybeToList)
import           Data.Text (Text)
import           Data.Time
import           Database.Beam hiding (insert)
import           Database.Beam.Query.Internal (QOrd, QNested)
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Syntax
import           Database.Beam.Backend.SQL.SQL92
import           Database.Beam.Backend.SQL
------------------------------------------------------------------------------
import           Chainweb.Api.ChainId
import           Chainweb.Api.Common (BlockHeight)
import           ChainwebData.Api
import           ChainwebData.Pagination
import           ChainwebDb.Database
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Transfer
import ChainwebDb.Types.Common (ReqKeyOrCoinbase)
------------------------------------------------------------------------------

type PgSelect a = SqlSelect Postgres (QExprToIdentity a)
type PgExpr s = QGenExpr QValueContext Postgres s
type PgBaseExpr = PgExpr QBaseScope

searchTxsQueryStmt
  :: Maybe Limit
  -> Maybe Offset
  -> Text
  -> SqlSelect
        Postgres
        (QExprToIdentity
                (QGenExpr QValueContext Postgres QBaseScope Int64,
                QGenExpr QValueContext Postgres QBaseScope Int64,
                QGenExpr QValueContext Postgres QBaseScope (DbHash BlockHash),
                QGenExpr QValueContext Postgres QBaseScope UTCTime,
                QGenExpr QValueContext Postgres QBaseScope (DbHash TxHash),
                QGenExpr QValueContext Postgres QBaseScope Text,
                (QGenExpr QValueContext Postgres QBaseScope (Maybe Text),
                QGenExpr QValueContext Postgres QBaseScope (Maybe (PgJSONB Value)),
                QGenExpr
                QValueContext Postgres QBaseScope (Maybe (PgJSONB Value)))))
searchTxsQueryStmt limit offset search =
    select $ do
        limit_ lim $ offset_ off $ orderBy_ (desc_ . getHeight) $ do
                tx <- all_ (_cddb_transactions database)
                guard_ (fromMaybe_ (val_ "") (_tx_code tx) `like_` (val_ searchString))
                return
                        ( (_tx_chainId tx)
                        , (_tx_height tx)
                        , (unBlockId $ _tx_block tx)
                        , (_tx_creationTime tx)
                        , (_tx_requestKey tx)
                        , (_tx_sender tx)
                        , ((_tx_code tx)
                        , (_tx_continuation tx)
                        , (_tx_goodResult tx)
                        ))
  where
    lim = maybe 10 (min 100 . unLimit) limit
    off = maybe 0 unOffset offset
    getHeight (_,a,_,_,_,_,_) = a
    searchString = "%" <> search <> "%"

data EventSearchParams = EventSearchParams
  { espSearch :: Maybe Text
  , espParam :: Maybe EventParam
  , espName :: Maybe EventName
  , espModuleName :: Maybe EventModuleName
  }

eventSearchCond ::
  EventSearchParams ->
  EventT (PgExpr s) ->
  PgExpr s Bool
eventSearchCond EventSearchParams{..} ev = foldr (&&.) (val_ True) $
  concat [searchCond, qualNameCond, paramCond, moduleCond]
  where
    searchString search = "%" <> search <> "%"
    searchCond = fromMaybeArg espSearch $ \s ->
      (_ev_qualName ev `like_` val_ (searchString s)) ||.
      (_ev_paramText ev `like_` val_ (searchString s))
    qualNameCond = fromMaybeArg espName $ \(EventName n) ->
      _ev_qualName ev `like_` val_ (searchString n)
    paramCond = fromMaybeArg espParam $ \(EventParam p) ->
      _ev_paramText ev `like_` val_ (searchString p)
    moduleCond = fromMaybeArg espModuleName $ \(EventModuleName m) ->
      _ev_module ev ==. val_ m
    fromMaybeArg mbA f = f <$> maybeToList mbA

data EventCursorT f = EventCursor
  { ecHeight :: C f Int64
  , ecReqKey :: C f ReqKeyOrCoinbase
  , ecIdx :: C f Int64
  } deriving (Generic, Beamable)

type EventCursor = EventCursorT Identity
deriving instance Show EventCursor

eventsCursorOrder ::
  EventT (PgExpr s) ->
  ( QOrd Postgres s Int64
  , QOrd Postgres s ReqKeyOrCoinbase
  , QOrd Postgres s Int64
  )
eventsCursorOrder ev =
  ( desc_ $ _ev_height ev
  , desc_ $ _ev_requestkey ev
  , asc_ $ _ev_idx ev
  )

eventAfterCursor ::
  EventCursor ->
  EventT (PgExpr s) ->
  PgExpr s Bool
eventAfterCursor EventCursor{..} ev = tupleCmp (<.)
  [ _ev_height ev :<> fromIntegral ecHeight
  , _ev_requestkey ev :<> val_ ecReqKey
  , negate (_ev_idx ev) :<> negate (fromIntegral ecIdx)
  ]

eventToCursor :: EventT f -> EventCursorT f
eventToCursor ev = EventCursor (_ev_height ev) (_ev_requestkey ev) (_ev_idx ev)

data EventQueryStart
  = EQLatest
  | EQFromHeight BlockHeight
  | EQFromCursor EventCursor

eventsAfterStart ::
  EventQueryStart ->
  Q Postgres ChainwebDataDb s (EventT (PgExpr s))
eventsAfterStart eqs = do
  ev <- all_ $ _cddb_events database
  case eqs of
    EQLatest -> return ()
    EQFromHeight hgt -> guard_ $ _ev_height ev <=. val_ (fromIntegral hgt)
    EQFromCursor cur -> guard_ $ eventAfterCursor cur ev
  return ev

eventsSearchOffset ::
  EventSearchParams ->
  EventQueryStart ->
  Offset ->
  Int64 ->
  SqlSelect Postgres (EventCursor, Int64, Int64)
eventsSearchOffset esp eqs =
  boundedScanOffset (eventSearchCond esp) (eventsAfterStart eqs) eventsCursorOrder eventToCursor

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

eventsSearchLimit ::
  EventSearchParams ->
  EventQueryStart ->
  Limit ->
  Int64 ->
  SqlSelect Postgres (Event, Block, Int64, Bool)
eventsSearchLimit esp eqs limit scanLimit = select $ do
  (ev, scan_num, matchingRow) <- boundedScanLimit
    (eventSearchCond esp) (eventsAfterStart eqs) eventsCursorOrder limit scanLimit
  blk <- all_ $ _cddb_blocks database
  guard_ $ _ev_block ev `references_` blk
  return (ev, blk, scan_num, matchingRow)

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

_bytequery :: Sql92SelectSyntax (BeamSqlBackendSyntax be) ~ PgSelectSyntax => SqlSelect be a -> ByteString
_bytequery = \case
  SqlSelect s -> pgRenderSyntaxScript $ fromPgSelect s

data AccountQueryStart
  = AQSNewQuery (Maybe BlockHeight) Offset
  | AQSContinue BlockHeight ReqKeyOrCoinbase Int

accountQueryStmt
    :: Limit
    -> Text
    -> Text
    -> Maybe ChainId
    -> AccountQueryStart
    -> SqlSelect
    Postgres
    (QExprToIdentity
    (TransferT (QGenExpr QValueContext Postgres QBaseScope)))
accountQueryStmt (Limit limit) account token chain aqs =
  select $
  limit_ limit $
  offset_ offset $
  orderBy_ getOrder $ do
    unionAll_ (accountQuery _tr_from_acct) (accountQuery _tr_to_acct)
  where
    getOrder tr =
      ( desc_ $ _tr_height tr
      , desc_ $ _tr_requestkey tr
      , asc_ $ _tr_idx tr)
    subQueryLimit = limit + offset
    accountQuery accountField = limit_ subQueryLimit $ orderBy_ getOrder $ do
      tr <- all_ (_cddb_transfers database)
      guard_ $ accountField tr ==. val_ account
      guard_ $ _tr_modulename tr ==. val_ token
      whenArg chain $ \(ChainId c) -> guard_ $ _tr_chainid tr ==. fromIntegral c
      rowFilter tr
      return tr
    (Offset offset, rowFilter) = case aqs of
      AQSNewQuery mbHeight ofst -> (,) ofst $ \tr ->
        whenArg mbHeight $ \bh -> guard_ $ _tr_height tr <=. val_ (fromIntegral bh)
      AQSContinue height reqKey idx -> (,) (Offset 0) $ \tr ->
        guard_ $ tupleCmp (<.)
          [ _tr_height tr :<> fromIntegral height
          , _tr_requestkey tr :<> val_ reqKey
          , negate (_tr_idx tr) :<> negate (fromIntegral idx)
          ]

data CompPair be s = forall t. (:<>) (QExpr be s t ) (QExpr be s t)

tupleCmp
  :: IsSql92ExpressionSyntax (BeamSqlBackendExpressionSyntax be)
  => (forall t. QExpr be s t -> QExpr be s t -> QExpr be s Bool)
  -> [CompPair be s]
  -> QExpr be s Bool
tupleCmp cmp cps = QExpr lExp `cmp` QExpr rExp where
  lExp = rowE <$> sequence [e | QExpr e :<> _ <- cps]
  rExp = rowE <$> sequence [e | _ :<> QExpr e <- cps]

whenArg :: Monad m => Maybe a -> (a -> m ()) -> m ()
whenArg p a = maybe (return ()) a p
