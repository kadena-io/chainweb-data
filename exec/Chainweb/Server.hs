{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Server where

------------------------------------------------------------------------------
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.Hash
import           Control.Concurrent
import           Control.Monad.Except
import           Data.Foldable
import           Data.IORef
import qualified Data.Pool as P
import           Data.Sequence (Seq)
import qualified Data.Sequence as S
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL
import           Database.Beam.Postgres
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Network.Wai.Middleware.Cors
import           Servant.API
import           Servant.Server
import           Text.Printf
------------------------------------------------------------------------------
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Listen
import           Chainweb.Lookups
import           ChainwebData.Types
import           ChainwebData.Api
import           ChainwebData.Pagination
import           ChainwebData.TxSummary
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction
------------------------------------------------------------------------------

setCors :: Middleware
setCors = cors . const . Just $ simpleCorsResourcePolicy
    { corsRequestHeaders = simpleHeaders
    }

apiServer :: Env -> IO ()
apiServer env = withPool (_env_dbConnectInfo env) $ \pool -> do
  recentTxs <- newIORef . RecentTxs . S.fromList =<< queryRecentTxs pool
  listenTid <- forkIO $ listenWithHandler env (serverHeaderHandler env pool recentTxs)
  Network.Wai.Handler.Warp.run 8080 $ setCors $ serve chainwebDataApi $
    recentTxsHandler recentTxs :<|>
    searchTxs pool

recentTxsHandler :: IORef RecentTxs -> Handler [TxSummary]
recentTxsHandler recentTxs = liftIO $ fmap (toList . _recentTxs_txs) $ readIORef recentTxs

serverHeaderHandler :: Env -> P.Pool Connection -> IORef RecentTxs -> PowHeader -> IO ()
serverHeaderHandler env pool recentTxs ph@(PowHeader h _) = do
  let pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
  payloadWithOutputs env pair >>= \case
    Nothing -> printf "[FAIL] Couldn't fetch parent for: %s\n"
      (hashB64U $ _blockHeader_hash h)
    Just pl -> do
      let chain = _blockHeader_chainId h
          height = _blockHeader_height h
          hash = _blockHeader_hash h
          ts = map (mkTxSummary chain height hash . fst) $ _blockPayloadWithOutputs_transactionsWithOutputs pl
          f rtx = (addNewTransactions (S.fromList ts) rtx, ())

      atomicModifyIORef' recentTxs f
      insertNewHeader pool ph pl


instance BeamSqlBackendIsString Postgres (Maybe Text)
instance BeamSqlBackendIsString Postgres (Maybe String)

searchTxs :: P.Pool Connection -> Maybe Limit -> Maybe Offset -> Maybe Text -> Handler [TxSummary]
searchTxs _ _ _ Nothing = do
    throwError $ err404 { errBody = "You must specify a search string" }
searchTxs pool limit offset (Just search) = do
    liftIO $ putStrLn $ "Transaction search: " <> T.unpack search
    liftIO $ P.withResource pool $ \c -> do
      res <- runBeamPostgresDebug putStrLn c $
        runSelectReturningList $ select $ do
        limit_ lim $ offset_ off $ orderBy_ (desc_ . getHeight) $ do
          tx <- all_ (_cddb_transactions database)
          blk <- all_ (_cddb_blocks database)
          guard_ (_tx_block tx `references_` blk)
          guard_ (_tx_code tx `like_` val_ (Just searchString))
          return
             ( (_tx_chainId tx)
             , (_block_height blk)
             , (unBlockId $ _tx_block tx)
             , (_tx_creationTime tx)
             , (_tx_requestKey tx)
             , (_tx_sender tx)
             , (_tx_code tx)
             , (_tx_goodResult tx)
             )
      return $ mkSummary <$> res
  where
    lim = maybe 10 (min 100 . unLimit) limit
    off = maybe 0 unOffset offset
    getHeight (_,a,_,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,g,h) = TxSummary a b (unDbHash c) d e f g (maybe TxFailed (const TxSucceeded) h)
    searchString = "%" <> search <> "%"

data h :. t = h :. t deriving (Eq,Ord,Show,Read,Typeable)
infixr 3 :.

type instance QExprToIdentity (a :. b) = (QExprToIdentity a) :. (QExprToIdentity b)
type instance QExprToField (a :. b) = (QExprToField a) :. (QExprToField b)


queryRecentTxs :: P.Pool Connection -> IO [TxSummary]
queryRecentTxs pool = do
    liftIO $ putStrLn "Getting recent transactions"
    P.withResource pool $ \c -> do
      res <- runBeamPostgresDebug putStrLn c $
        runSelectReturningList $ select $ do
        limit_ 20 $ orderBy_ (desc_ . getHeight) $ do
          tx <- all_ (_cddb_transactions database)
          blk <- all_ (_cddb_blocks database)
          guard_ (_tx_block tx `references_` blk)
          return
             ( (_tx_chainId tx)
             , (_block_height blk)
             , (unBlockId $ _tx_block tx)
             , (_tx_creationTime tx)
             , (_tx_requestKey tx)
             , (_tx_sender tx)
             , (_tx_code tx)
             , (_tx_goodResult tx)
             )
      return $ mkSummary <$> res
  where
    getHeight (_,a,_,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,g,h) = TxSummary a b (unDbHash c) d e f g (maybe TxFailed (const TxSucceeded) h)

data RecentTxs = RecentTxs
  { _recentTxs_txs :: Seq TxSummary
  } deriving (Eq,Ord,Show)

getSummaries :: RecentTxs -> [TxSummary]
getSummaries (RecentTxs s) = toList s

addNewTransactions :: Seq TxSummary -> RecentTxs -> RecentTxs
addNewTransactions txs (RecentTxs s1) = RecentTxs s2
  where
    maxTransactions = 10
    s2 = S.take maxTransactions $ txs <> s1
