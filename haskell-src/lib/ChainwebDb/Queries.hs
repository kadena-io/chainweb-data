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
import           Data.Functor ((<&>))
import           Data.Maybe (maybeToList)
import           Data.Text (Text)
import           Data.Time
import           Data.Vector (Vector)
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

data HeightRangeParams = HeightRangeParams
  { hrpMinHeight :: Maybe BlockHeight
  , hrpMaxHeight :: Maybe BlockHeight
  }

guardInRange :: HeightRangeParams -> PgExpr s Int64 -> Q Postgres db s ()
guardInRange HeightRangeParams{..} hgt = do
  whenArg hrpMinHeight $ \h -> guard_ $ hgt >=. val_ (fromIntegral h)
  whenArg hrpMaxHeight $ \h -> guard_ $ hgt <=. val_ (fromIntegral h)

-- | A subset of the TransactionT record, used with beam for querying the
-- /txs/search endpoint response payload
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

data TxCursorT f = TxCursor
  { txcHeight :: C f Int64
  , txcReqKey :: C f (DbHash TxHash)
  } deriving (Generic, Beamable)

type TxCursor = TxCursorT Identity

toTxSearchCursor :: DbTxSummaryT f -> TxCursorT (Directional f)
toTxSearchCursor DbTxSummary{..} = TxCursor
  (desc dtsHeight)
  (desc dtsReqKey)

toDbTxSummary :: TransactionT f -> DbTxSummaryT f
toDbTxSummary Transaction{..} = DbTxSummary
  { dtsChainId = _tx_chainId
  , dtsHeight = _tx_height
  , dtsBlock = unBlockId _tx_block
  , dtsCreationTime = _tx_creationTime
  , dtsReqKey = _tx_requestKey
  , dtsSender = _tx_sender
  , dtsCode = _tx_code
  , dtsContinuation = _tx_continuation
  , dtsGoodResult = _tx_goodResult
   }

data ContinuationHistoryT f = ContinuationHistory
  { chCode :: C f (Maybe Text)
  , chSteps :: C f (Vector Text)
  } deriving (Generic, Beamable)

type ContinuationHistory = ContinuationHistoryT Identity

deriving instance Show ContinuationHistory

joinContinuationHistory :: PgExpr s (Maybe (DbHash TxHash)) ->
  Q Postgres ChainwebDataDb s (ContinuationHistoryT (PgExpr s))
joinContinuationHistory pactIdExp = pgUnnest $ (customExpr_ $ \pactId ->
  -- We need the following LATERAL keyword so that it can be used liberally
  -- in any Q context despite the fact that it refers to the `pactIdExp` coming
  -- from the outside scope. The LATERAL helps, because when the expression below
  -- appears after a XXXX JOIN, this LATERAL prefix will turn it into a lateral
  -- join. This is very hacky, but Postgres allows the LATERAL keyword after FROM
  -- as well, so I can't think of a case that would cause the hack to blow up.
  -- Either way, once we have migrations going, we should replace this body with
  -- a Postgres function call, which the pgUnnest + customExpr_ combination was
  -- designed for.
  "LATERAL ( " <>
    "WITH RECURSIVE transactionSteps AS ( " <>
      "SELECT DISTINCT ON (depth) tInner.code, tInner.pactid, 1 AS depth, tInner.requestkey " <>
      "FROM transactions AS tInner " <>
      "WHERE (tInner.requestkey) = " <> pactId <>
      "UNION ALL " <>
      "SELECT DISTINCT ON (depth) tInner.code, tInner.pactid, tRec.depth + 1, tInner.requestkey " <>
      "FROM transactions AS tInner " <>
      "INNER JOIN transactionSteps AS tRec ON tRec.pactid = tInner.requestkey " <>
    ")" <>
    "SELECT (array_agg(code) FILTER (WHERE code IS NOT NULL))[1] as code " <>
         ", array_agg(requestkey ORDER BY depth) as steps " <>
    "FROM transactionSteps " <>
  ")") pactIdExp

data TxSummaryWithHistoryT f = TxSummaryWithHistory
  { txwhSummary :: DbTxSummaryT f
  , txwhContHistory :: ContinuationHistoryT f
  } deriving (Generic, Beamable)

type TxSummaryWithHistory = TxSummaryWithHistoryT Identity

txSearchSource ::
  Text ->
  HeightRangeParams ->
  Q Postgres ChainwebDataDb s (FilterMarked TxSummaryWithHistoryT (PgExpr s))
txSearchSource search hgtRange = do
  tx <- all_ $ _cddb_transactions database
  contHist <- joinContinuationHistory (_tx_pactId tx)
  let codeMerged = coalesce_
        [ just_ $ _tx_code tx
        , just_ $ chCode contHist
        ]
        nothing_
  guardInRange hgtRange (_tx_height tx)
  let searchExp = val_ ("%" <> search <> "%")
      isMatch = fromMaybe_ (val_ "") codeMerged `like_` searchExp
  return $ FilterMarked isMatch $
    TxSummaryWithHistory (toDbTxSummary tx) contHist

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
eventSearchCond EventSearchParams{..} ev = and_ $
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

type EventQueryStart = Maybe BlockHeight

toEventsSearchCursor :: EventT f -> EventCursorT (Directional f)
toEventsSearchCursor Event{..} = EventCursor
  (desc _ev_height)
  (desc _ev_requestkey)
  (asc _ev_idx)

eventsSearchSource ::
  EventSearchParams ->
  HeightRangeParams ->
  Q Postgres ChainwebDataDb s (FilterMarked EventT (PgExpr s))
eventsSearchSource esp hgtRange = do
  ev <- all_ $ _cddb_events database
  guardInRange hgtRange (_ev_height ev)
  return $ FilterMarked (eventSearchCond esp ev) ev

newtype EventSearchExtrasT f = EventSearchExtras
  { eseBlockTime :: C f UTCTime
  } deriving (Generic, Beamable)

eventSearchExtras ::
  EventT (PgExpr s) ->
  Q Postgres ChainwebDataDb s (EventSearchExtrasT (PgExpr s))
eventSearchExtras ev = do
  blk <- all_ $ _cddb_blocks database
  guard_ $ _ev_block ev `references_` blk
  return $ EventSearchExtras
    { eseBlockTime = _block_creationTime blk
    }

_bytequery :: Sql92SelectSyntax (BeamSqlBackendSyntax be) ~ PgSelectSyntax => SqlSelect be a -> ByteString
_bytequery = \case
  SqlSelect s -> pgRenderSyntaxScript $ fromPgSelect s

data AccountQueryStart
  = AQSNewQuery Offset
  | AQSContinue BlockHeight ReqKeyOrCoinbase Int

toAccountsSearchCursor :: TransferT f -> EventCursorT (Directional f)
toAccountsSearchCursor Transfer{..} = EventCursor
  (desc _tr_height)
  (desc _tr_requestkey)
  (asc _tr_idx)

data TransferSearchParams = TransferSearchParams
  { tspToken :: Text
  , tspChainId :: Maybe ChainId
  , tspHeightRange :: HeightRangeParams
  , tspAccount :: Text
  }

transfersSearchSource ::
  TransferSearchParams ->
  Q Postgres ChainwebDataDb s (FilterMarked TransferT (PgExpr s))
transfersSearchSource tsp = do
    tr <- sourceTransfersScan
    return $ FilterMarked (searchCond tr) tr
  where
    tokenCond tr = _tr_modulename tr ==. val_ (tspToken tsp)
    chainCond tr = tspChainId tsp <&> \(ChainId c) -> _tr_chainid tr ==. fromIntegral c
    searchCond tr = and_ $ tokenCond tr : maybeToList (chainCond tr)
    getOrder tr =
      ( desc_ $ _tr_height tr
      , desc_ $ _tr_requestkey tr
      , asc_ $ _tr_idx tr)
    accountQuery accountField = orderBy_ getOrder $ do
      tr <- all_ (_cddb_transfers database)
      guard_ $ accountField tr ==. val_ (tspAccount tsp)
      guardInRange (tspHeightRange tsp) (_tr_height tr)
      return tr
    sourceTransfersScan = unionAll_ (accountQuery _tr_from_acct) (accountQuery _tr_to_acct)

data TransferSearchExtrasT f = TransferSearchExtras
  { tseBlockTime :: C f UTCTime
  , tseXChainAccount :: C f (Maybe Text)
  , tseXChainId :: C f (Maybe Int64)
  } deriving (Generic, Beamable)

transferSearchExtras ::
  TransferT (PgExpr s) ->
  Q Postgres ChainwebDataDb s (TransferSearchExtrasT (PgExpr s))
transferSearchExtras tr = do
  blk <- all_ $ _cddb_blocks database
  guard_ $ _tr_block tr `references_` blk
  xChainInfo <- joinXChainInfo tr
  return $ TransferSearchExtras
    { tseBlockTime = _block_creationTime blk
    , tseXChainAccount = xciAccount xChainInfo
    , tseXChainId = xciChainId xChainInfo
    }

data XChainInfoT f = XChainInfo
  { xciAccount :: C f (Maybe Text)
  , xciChainId :: C f (Maybe Int64)
  } deriving (Generic, Beamable)

joinXChainInfo :: TransferT (PgExpr s) ->
  Q Postgres ChainwebDataDb s (XChainInfoT (PgExpr s))
joinXChainInfo tr = pgUnnest $ (customExpr_ $ \fromAcct toAcct idx mdName blk req amt ->
  -- We need the following LATERAL keyword so that it can be used liberally
  -- in any Q context despite the fact that it refers to the `tr` coming
  -- from the outside scope. The LATERAL helps, because when the expression below
  -- appears after a XXXX JOIN, this LATERAL prefix will turn it into a lateral
  -- join. This is very hacky, but Postgres allows the LATERAL keyword after FROM
  -- as well, so I can't think of a case that would cause the hack to blow up.
  -- Either way, once we have migrations going, we should replace this body with
  -- a Postgres function call, which the pgUnnest + customExpr_ combination was
  -- designed for.
  " LATERAL ( " <>
    " SELECT e.params->>1 AS acct, CAST(e.params->>3 AS INT) " <>
    " FROM events e " <>
    " WHERE e.block = " <> blk <>
      " AND e.requestkey = " <> req <>
      " AND e.qualname = 'coin.TRANSFER_XCHAIN' " <>
      " AND e.params->>0 = " <> fromAcct <>
      " AND " <> toAcct <> " = '' " <>
      " AND e.idx = " <> idx <> " - 1 " <>
      " AND " <> mdName <> " = 'coin' " <>
    " UNION ALL " <>
    " SELECT e.params->2->>0 AS acct, CAST(e.params->>0 AS INT) AS chainid " <>
    " FROM events e " <>
    " WHERE " <> mdName <> " = 'coin' " <>
      " AND " <> req <> " != 'cb' " <>
      " AND " <> fromAcct <> " = '' " <>
      " AND e.block = " <> blk <>
      " AND e.requestkey = " <> req <>
      " AND e.qualname = 'pact.X_RESUME' " <>
      " AND e.params->>1 = 'coin.transfer-crosschain' " <>
      " AND e.params->2->>1 = " <> toAcct <>
    " UNION ALL " <>
    " SELECT NULL, NULL " <>
    " LIMIT 1 " <>
  ")")
  (_tr_from_acct tr)
  (_tr_to_acct tr)
  (_tr_idx tr)
  (_tr_modulename tr)
  (unBlockId $ _tr_block tr)
  (_tr_requestkey tr)
  (_tr_amount tr)

whenArg :: Monad m => Maybe a -> (a -> m ()) -> m ()
whenArg p a = maybe (return ()) a p

and_ :: BeamSqlBackend be => [QExpr be s Bool] -> QExpr be s Bool
and_ [] = val_ True
and_ (cond:conds) = foldr (&&.) cond conds
