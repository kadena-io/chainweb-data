{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
-- |

module ChainwebDb.Queries (eventsQueryStmt, searchTxsQueryStmt) where

------------------------------------------------------------------------------
import           Data.Aeson hiding (Error)
import           Data.Coerce
import           Data.Int
import           Data.Text (Text)
import           Data.Time
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
------------------------------------------------------------------------------
import           ChainwebData.Api
import           ChainwebDb.Database
import           ChainwebData.Pagination
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Transaction
------------------------------------------------------------------------------

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
                blk <- all_ (_cddb_blocks database)
                guard_ (_tx_block tx `references_` blk)
                guard_ (isJust_ $ _tx_code tx)
                guard_ (fromMaybe_ (val_ "") (_tx_code tx) `like_` val_ searchString)
                return
                        ( (_tx_chainId tx)
                        , (_block_height blk)
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

eventsQueryStmt :: Maybe Limit -> Maybe Offset -> Maybe Text -> Maybe EventParam -> Maybe EventName
                -> SqlSelect
                   Postgres
                   (QExprToIdentity
                    (TransactionT (QGenExpr QValueContext Postgres QBaseScope)
                    , BlockT (QGenExpr QValueContext Postgres QBaseScope)
                    , EventT (QGenExpr QValueContext Postgres QBaseScope)))
eventsQueryStmt limit offset qSearch qParam qName =
  select $
    limit_ lim $ offset_ off $ orderBy_ getOrder $ do
      tx <- all_ (_cddb_transactions database)
      blk <- all_ (_cddb_blocks database)
      ev <- all_ (_cddb_events database)
      guard_ (_tx_block tx `references_` blk)
      guard_ (TransactionId (coerce $ _ev_requestkey ev) (_tx_block tx) `references_` tx)
      whenArg qSearch $ \s -> guard_
        ((_ev_qualName ev `like_` val_ (searchString s)) ||.
         (_ev_paramText ev `like_` val_ (searchString s))
        )
      whenArg qName $ \(EventName n) -> guard_ (_ev_qualName ev `like_` val_ (searchString n))
      whenArg qParam $ \(EventParam p) -> guard_ (_ev_paramText ev `like_` val_ (searchString p))
      return (tx,blk,ev)
  where
    whenArg p a = maybe (return ()) a p
    lim = maybe 10 (min 100 . unLimit) limit
    off = maybe 0 unOffset offset
    getOrder (tx,blk,ev) =
      (desc_ $ _block_height blk
      ,asc_ $ _tx_chainId tx
      ,desc_ $ _tx_txid tx
      ,asc_ $ _ev_idx ev)
    searchString search = "%" <> search <> "%"
