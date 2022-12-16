-- We're disabling the missing-signatures warning because some Beam types are
-- too unwieldy to type and some types aren't even exported so they can't even
-- be typed explicitly.
{-# OPTIONS_GHC -Wno-missing-signatures #-}

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
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Syntax
import           Database.Beam.Backend.SQL.SQL92
import           Database.Beam.Backend.SQL
------------------------------------------------------------------------------
import           Chainweb.Api.ChainId
import           Chainweb.Api.Common (BlockHeight)
import           ChainwebData.Api
import           ChainwebData.Pagination
import           ChainwebDb.BoundedScan
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

data DbTxSummaryT f = DbTxSummary
  { dtsChainId :: C f Int64
  , dtsHeight :: C f Int64
  , dtsBlock :: C f (DbHash BlockHash)
  , dtsCreationTime :: C f UTCTime
  , dtsReqKey :: C f (DbHash TxHash)
  , dtsSender :: C f Text
  , dtsCode :: C f (Maybe Text)
  , dtsContinuation :: C f (Maybe (PgJSONB Value))
  , dtsGoodResult :: C f (Maybe (PgJSONB Value))
  } deriving (Generic, Beamable)

type DbTxSummary = DbTxSummaryT Identity

txToSummary :: TransactionT f -> DbTxSummaryT f
txToSummary tx = DbTxSummary
  { dtsChainId = _tx_chainId tx
  , dtsHeight = _tx_height tx
  , dtsBlock = unBlockId $ _tx_block tx
  , dtsCreationTime = _tx_creationTime tx
  , dtsReqKey = _tx_requestKey tx
  , dtsSender = _tx_sender tx
  , dtsCode = _tx_code tx
  , dtsContinuation = _tx_continuation tx
  , dtsGoodResult = _tx_goodResult tx
  }

data TxCursorT f = TxCursor
  { txcHeight :: C f Int64
  , txcReqKey :: C f (DbHash TxHash)
  } deriving (Generic, Beamable)

type TxCursor = TxCursorT Identity

type TxQueryStart = BSStart () TxCursor

txSearchCond :: Text -> TransactionT (PgExpr s) -> PgExpr s Bool
txSearchCond search tx =
  fromMaybe_ (val_ "") (_tx_code tx) `like_` val_ searchString
  where searchString = "%" <> search <> "%"

txSearchAfter :: TxQueryStart -> Q Postgres ChainwebDataDb s (TransactionT (PgExpr s))
txSearchAfter bss = do
  tx <- all_ $ _cddb_transactions database
  case bss of
    BSNewQuery () -> return ()
    BSFromCursor TxCursor{..} -> guard_ $ tupleCmp (<.)
      [ _tx_height tx :<> fromIntegral txcHeight
      , _tx_requestKey tx :<> val_ txcReqKey
      ]
  return tx

txSearchOrder tx = (desc_ $ _tx_height tx, desc_ $ _tx_requestKey tx)

txToCursor :: TransactionT f -> TxCursorT f
txToCursor tx = TxCursor (_tx_height tx) (_tx_requestKey tx)

txSearchOffset ::
  Text ->
  TxQueryStart ->
  Offset ->
  Int64 ->
  SqlSelect Postgres (TxCursor, Int64, Int64)
txSearchOffset search tqs o l = select $ boundedScanOffset
  (txSearchCond search) (txSearchAfter tqs) txSearchOrder txToCursor o l

txSearchLimit ::
  Text ->
  TxQueryStart ->
  Limit ->
  Int64 ->
  SqlSelect Postgres (DbTxSummary, Int64, Bool)
txSearchLimit search tqs limit scanLimit = select $ do
  (tx, scan_num, matchingRow) <- boundedScanLimit
    (txSearchCond search) (txSearchAfter tqs) txSearchOrder limit scanLimit
  return (txToSummary tx, scan_num, matchingRow)

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

type EventQueryStart = BSStart (Maybe BlockHeight) EventCursor

eventsAfterStart ::
  EventQueryStart ->
  Q Postgres ChainwebDataDb s (EventT (PgExpr s))
eventsAfterStart eqs = do
  ev <- all_ $ _cddb_events database
  case eqs of
    BSNewQuery Nothing -> return ()
    BSNewQuery (Just hgt) -> guard_ $ _ev_height ev >=. val_ (fromIntegral hgt)
    BSFromCursor cur -> guard_ $ eventAfterCursor cur ev
  return ev

eventsSearchOffset ::
  EventSearchParams ->
  EventQueryStart ->
  Offset ->
  Int64 ->
  SqlSelect Postgres (EventCursor, Int64, Int64)
eventsSearchOffset esp eqs o l = select $ boundedScanOffset
  (eventSearchCond esp) (eventsAfterStart eqs) eventsCursorOrder eventToCursor o l

eventsSearchLimit ::
  EventSearchParams ->
  EventQueryStart ->
  Limit ->
  Int64 ->
  SqlSelect Postgres ((Event, Block), Int64, Bool)
eventsSearchLimit esp eqs limit scanLimit = select $ do
  (ev, scan_num, matchingRow) <- boundedScanLimit
    (eventSearchCond esp) (eventsAfterStart eqs) eventsCursorOrder limit scanLimit
  blk <- all_ $ _cddb_blocks database
  guard_ $ _ev_block ev `references_` blk
  return ((ev, blk), scan_num, matchingRow)

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
