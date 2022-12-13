{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
-- |

module ChainwebDb.Queries where

------------------------------------------------------------------------------
import           Data.Aeson hiding (Error)
import           Data.ByteString.Lazy (ByteString)
import           Data.Int
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

eventSearch ::
  EventSearchParams ->
  Q Postgres ChainwebDataDb s (EventT (PgExpr s))
eventSearch EventSearchParams{..} = do
  ev <- all_ (_cddb_events database)
  whenArg espSearch $ \s -> guard_
    ((_ev_qualName ev `like_` val_ (searchString s)) ||.
      (_ev_paramText ev `like_` val_ (searchString s))
    )
  whenArg espName $ \(EventName n) -> guard_ (_ev_qualName ev `like_` val_ (searchString n))
  whenArg espParam $ \(EventParam p) -> guard_ (_ev_paramText ev `like_` val_ (searchString p))
  whenArg espModuleName $ \(EventModuleName m) -> guard_ (_ev_module ev ==. val_ m)
  return ev
  where
    searchString search = "%" <> search <> "%"

eventsQueryFull ::
  EventSearchParams ->
  Maybe Int -> -- BlockHeight
  Q Postgres ChainwebDataDb s (BlockT (PgExpr s), EventT (PgExpr s))
eventsQueryFull esp bh = orderBy_ getOrder $ do
    blk <- all_ (_cddb_blocks database)
    ev <- eventSearch esp
    guard_ (_ev_block ev `references_` blk)
    whenArg bh $ \bh' -> guard_ (_ev_height ev >=. val_ (fromIntegral bh'))
    return (blk, ev)
  where
    getOrder (_,ev) =
      (desc_ $ _ev_height ev
      ,asc_ $ _ev_chainid ev
      ,asc_ $ _ev_idx ev)

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
