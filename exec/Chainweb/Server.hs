{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Server where

------------------------------------------------------------------------------
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.ChainId
import           Chainweb.Api.Hash
import           Control.Applicative
import           Control.Concurrent
import           Control.Error
import           Control.Monad.Except
import           Data.Aeson hiding (Error)
import           Data.ByteString.Lazy (ByteString)
import           Data.Coerce
import           Data.Foldable
import           Data.Int
import           Data.IORef
import qualified Data.Pool as P
import           Data.Proxy
import           Data.Sequence (Seq)
import qualified Data.Sequence as S
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL
import           Database.Beam.Postgres
import           Control.Lens
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Network.Wai.Middleware.Cors
import           Servant.API
import           Servant.Server
import           System.Directory
import           System.FilePath
import           System.Logger.Types hiding (logg)
import           Text.Printf
------------------------------------------------------------------------------
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Coins
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Listen
import           Chainweb.Lookups
import           Chainweb.RichList
import           ChainwebData.Types
import           ChainwebData.Api
import           ChainwebData.EventDetail
import           ChainwebData.Pagination
import           ChainwebData.TxDetail
import           ChainwebData.TxSummary
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Event
------------------------------------------------------------------------------

setCors :: Middleware
setCors = cors . const . Just $ simpleCorsResourcePolicy
    { corsRequestHeaders = simpleHeaders
    }

data ServerState = ServerState
    { _ssRecentTxs :: RecentTxs
    , _ssHighestBlockHeight :: BlockHeight
    , _ssTransactionCount :: Maybe Int64
    , _ssCirculatingCoins :: Maybe Double
    } deriving (Eq,Show)

ssRecentTxs
    :: Functor f
    => (RecentTxs -> f RecentTxs)
    -> ServerState -> f ServerState
ssRecentTxs = lens _ssRecentTxs setter
  where
    setter sc v = sc { _ssRecentTxs = v }

ssHighestBlockHeight
    :: Functor f
    => (BlockHeight -> f BlockHeight)
    -> ServerState -> f ServerState
ssHighestBlockHeight = lens _ssHighestBlockHeight setter
  where
    setter sc v = sc { _ssHighestBlockHeight = v }

ssTransactionCount
    :: Functor f
    => (Maybe Int64 -> f (Maybe Int64))
    -> ServerState -> f ServerState
ssTransactionCount = lens _ssTransactionCount setter
  where
    setter sc v = sc { _ssTransactionCount = v }

ssCirculatingCoins
    :: Functor f
    => (Maybe Double -> f (Maybe Double))
    -> ServerState -> f ServerState
ssCirculatingCoins = lens _ssCirculatingCoins setter
  where
    setter sc v = sc { _ssCirculatingCoins = v }

type RichlistEndpoint = "richlist.csv" :> Get '[PlainText] Text

type TxEndpoint = "tx" :> QueryParam "requestkey" Text :> Get '[JSON] TxDetail

type TheApi =
  ChainwebDataApi
  :<|> RichlistEndpoint
theApi :: Proxy TheApi
theApi = Proxy

apiServer :: Env -> ServerEnv -> IO ()
apiServer env senv = do
  ecut <- queryCut env
  let logg = _env_logger env
  case ecut of
    Left e -> do
       logg Error "Error querying cut"
       logg Info $ fromString $ show e
    Right cutBS -> apiServerCut env senv cutBS

apiServerCut :: Env -> ServerEnv -> ByteString -> IO ()
apiServerCut env senv cutBS = do
  let curHeight = cutMaxHeight cutBS
      logg = _env_logger env
  circulatingCoins <- queryCirculatingCoins env (fromIntegral curHeight)
  logg Info $ fromString $ "Total coins in circulation: " <> show circulatingCoins
  let pool = _env_dbConnPool env
  recentTxs <- RecentTxs . S.fromList <$> queryRecentTxs (logFunc senv env) pool
  numTxs <- getTransactionCount (logFunc senv env) pool
  ssRef <- newIORef $ ServerState recentTxs 0 numTxs (hush circulatingCoins)
  _ <- forkIO $ scheduledUpdates env senv pool ssRef
  _ <- forkIO $ listenWithHandler env $
    serverHeaderHandler env (_serverEnv_verbose senv) pool ssRef
  Network.Wai.Handler.Warp.run (_serverEnv_port senv) $ setCors $ serve theApi $
    ( ( recentTxsHandler ssRef
        :<|> searchTxs (logFunc senv env) pool
        :<|> evHandler (logFunc senv env) pool
        :<|> txHandler (logFunc senv env) pool
      )
        :<|> statsHandler ssRef
        :<|> coinsHandler ssRef
      )
      :<|> richlistHandler

scheduledUpdates
  :: Env
  -> ServerEnv
  -> P.Pool Connection
  -> IORef ServerState
  -> IO ()
scheduledUpdates env senv pool ssRef = forever $ do
    threadDelay (60 * 60 * 24 * micros)

    now <- getCurrentTime
    logg Info $ fromString $ show now
    logg Info "Recalculating coins in circulation:"
    height <- _ssHighestBlockHeight <$> readIORef ssRef
    circulatingCoins <- queryCirculatingCoins env height
    logg Info $ fromString $ show circulatingCoins
    let f ss = (ss & ssCirculatingCoins %~ (hush circulatingCoins <|>), ())
    atomicModifyIORef' ssRef f

    numTxs <- getTransactionCount (logFunc senv env) pool
    logg Info $ fromString $ "Updated number of transactions: " <> show numTxs
    let g ss = (ss & ssTransactionCount %~ (numTxs <|>), ())
    atomicModifyIORef' ssRef g

    h <- getHomeDirectory
    richList logg (h </> ".local/share")
    logg Info "Updated rich list"
  where
    micros = 1000000
    logg = _env_logger env

richlistHandler :: Handler Text
richlistHandler = do
  let f = "richlist.csv"
  exists <- liftIO $ doesFileExist f
  if exists
    then liftIO $ T.readFile f
    else throwError err404

coinsHandler :: IORef ServerState -> Handler Text
coinsHandler ssRef = liftIO $ fmap mkStats $ readIORef ssRef
  where
    mkStats ss = maybe "" (T.pack . show) $ _ssCirculatingCoins ss

statsHandler :: IORef ServerState -> Handler ChainwebDataStats
statsHandler ssRef = liftIO $ do
    fmap mkStats $ readIORef ssRef
  where
    mkStats ss = ChainwebDataStats (fromIntegral <$> _ssTransactionCount ss)
                                   (_ssCirculatingCoins ss)

recentTxsHandler :: IORef ServerState -> Handler [TxSummary]
recentTxsHandler ss = liftIO $ fmap (toList . _recentTxs_txs . _ssRecentTxs) $ readIORef ss

serverHeaderHandler :: Env -> Bool -> P.Pool Connection -> IORef ServerState -> PowHeader -> IO ()
serverHeaderHandler env verbose pool ssRef ph@(PowHeader h _) = do
  let chain = _blockHeader_chainId h
  let height = _blockHeader_height h
  let pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
  let logg = _env_logger env
  payloadWithOutputs env pair >>= \case
    Left e -> do
      logg Error $ fromString $ printf "Couldn't fetch parent for: %s\n"
        (hashB64U $ _blockHeader_hash h)
      logg Info $ fromString $ show e
    Right pl -> do
      let hash = _blockHeader_hash h
          tos = _blockPayloadWithOutputs_transactionsWithOutputs pl
          ts = S.fromList $ map (\(t,tout) -> mkTxSummary chain height hash t tout) tos
          f ss = (ss & ssRecentTxs %~ addNewTransactions ts
                     & ssHighestBlockHeight %~ max height
                     & (ssTransactionCount . _Just) +~ (fromIntegral $ S.length ts), ())

      let msg = printf "Got new header on chain %d height %d" (unChainId chain) height
          addendum = if S.length ts == 0
                       then ""
                       else printf " with %d transactions" (S.length ts)
      when verbose $ do
        logg Info (fromString $ msg <> addendum)
        mapM_ (logg Info . fromString . show) tos

      atomicModifyIORef' ssRef f
      insertNewHeader pool ph pl


instance BeamSqlBackendIsString Postgres (Maybe Text)
instance BeamSqlBackendIsString Postgres (Maybe String)

searchTxs
  :: (LogLevel -> String -> IO ())
  -> P.Pool Connection
  -> Maybe Limit
  -> Maybe Offset
  -> Maybe Text
  -> Handler [TxSummary]
searchTxs _ _ _ _ Nothing = throw404 "You must specify a search string"
searchTxs logger pool limit offset (Just search) = do
    liftIO $ logger Info $ "Transaction search: " <> T.unpack search
    liftIO $ P.withResource pool $ \c -> do
      res <- runBeamPostgresDebug (logger Debug . fromString) c $
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
             , ((_tx_code tx)
             , (_tx_continuation tx)
             , (_tx_goodResult tx)
             ))
      return $ mkSummary <$> res
  where
    lim = maybe 10 (min 100 . unLimit) limit
    off = maybe 0 unOffset offset
    getHeight (_,a,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,(g,h,i)) = TxSummary (fromIntegral a) (fromIntegral b) (unDbHash c) d e f g (unPgJsonb <$> h) (maybe TxFailed (const TxSucceeded) i)
    searchString = "%" <> search <> "%"

throw404 :: MonadError ServerError m => ByteString -> m a
throw404 msg = throwError $ err404 { errBody = msg }

txHandler
  :: (LogLevel -> String -> IO ())
  -> P.Pool Connection
  -> Maybe RequestKey
  -> Handler TxDetail
txHandler _ _ Nothing = throw404 "You must specify a search string"
txHandler logger pool (Just (RequestKey rk)) =
  may404 $ liftIO $ P.withResource pool $ \c ->
  runBeamPostgresDebug (logger Debug) c $ do
    r <- runSelectReturningOne $ select $ do
      tx <- all_ (_cddb_transactions database)
      blk <- all_ (_cddb_blocks database)
      guard_ (_tx_block tx `references_` blk)
      guard_ (_tx_requestKey tx ==. val_ rk)
      return (tx,blk)
    evs <- runSelectReturningList $ select $ do
       ev <- all_ (_cddb_events database)
       guard_ (_ev_requestkey ev ==. val_ (Just $ DbHash rk))
       return ev
    return $ (`fmap` r) $ \(tx,blk) -> TxDetail
        { _txDetail_ttl = fromIntegral $ _tx_ttl tx
        , _txDetail_gasLimit = fromIntegral $ _tx_gasLimit tx
        , _txDetail_gasPrice = _tx_gasPrice tx
        , _txDetail_nonce = _tx_nonce tx
        , _txDetail_pactId = _tx_pactId tx
        , _txDetail_rollback = _tx_rollback tx
        , _txDetail_step = fromIntegral <$> _tx_step tx
        , _txDetail_data = unMaybeValue $ _tx_data tx
        , _txDetail_proof = _tx_proof tx
        , _txDetail_gas = fromIntegral $ _tx_gas tx
        , _txDetail_result =
          maybe (unMaybeValue $ _tx_badResult tx) unPgJsonb $
          _tx_goodResult tx
        , _txDetail_logs = fromMaybe "" $ _tx_logs tx
        , _txDetail_metadata = unMaybeValue $ _tx_metadata tx
        , _txDetail_continuation = unPgJsonb <$> _tx_continuation tx
        , _txDetail_txid = maybe 0 fromIntegral $ _tx_txid tx
        , _txDetail_chain = fromIntegral $ _tx_chainId tx
        , _txDetail_height = fromIntegral $ _block_height blk
        , _txDetail_blockTime = _block_creationTime blk
        , _txDetail_blockHash = unDbHash $ unBlockId $ _tx_block tx
        , _txDetail_creationTime = _tx_creationTime tx
        , _txDetail_requestKey = _tx_requestKey tx
        , _txDetail_sender = _tx_sender tx
        , _txDetail_code = _tx_code tx
        , _txDetail_success =
          maybe False (const True) $ _tx_goodResult tx
        , _txDetail_events = map toTxEvent evs
        }

  where
    unMaybeValue = maybe Null unPgJsonb
    toTxEvent ev =
      TxEvent (_ev_qualName ev) (unPgJsonb $ _ev_params ev)
    may404 a = a >>= maybe (throw404 "Tx not found") return

evHandler
  :: (LogLevel -> String -> IO ())
  -> P.Pool Connection
  -> Maybe Limit
  -> Maybe Offset
  -> Maybe Text -- ^ fulltext search
  -> Maybe EventParam
  -> Maybe EventName
  -> Handler [EventDetail]
evHandler logger pool limit offset qSearch qParam qName =
  liftIO $ P.withResource pool $ \c -> do
    r <- runBeamPostgresDebug (logger Debug) c $
      runSelectReturningList $ select $
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
    return $ (`map` r) $ \(tx,blk,ev) -> EventDetail
      { _evDetail_name = _ev_qualName ev
      , _evDetail_params = unPgJsonb $ _ev_params ev
      , _evDetail_moduleHash = _ev_moduleHash ev
      , _evDetail_chain = fromIntegral $ _tx_chainId tx
      , _evDetail_height = fromIntegral $ _block_height blk
      , _evDetail_blockTime = _block_creationTime blk
      , _evDetail_blockHash = unDbHash $ unBlockId $ _tx_block tx
      , _evDetail_requestKey = _tx_requestKey tx
      , _evDetail_idx = fromIntegral $ _ev_idx ev
      }
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

data h :. t = h :. t deriving (Eq,Ord,Show,Read,Typeable)
infixr 3 :.

type instance QExprToIdentity (a :. b) = (QExprToIdentity a) :. (QExprToIdentity b)
type instance QExprToField (a :. b) = (QExprToField a) :. (QExprToField b)


queryRecentTxs :: (LogLevel -> String -> IO ()) -> P.Pool Connection -> IO [TxSummary]
queryRecentTxs logger pool = do
    liftIO $ logger Info "Getting recent transactions"
    P.withResource pool $ \c -> do
      res <- runBeamPostgresDebug (logger Debug) c $
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
             , ((_tx_code tx)
             , (_tx_continuation tx)
             , (_tx_goodResult tx)
             ))
      return $ mkSummary <$> res
  where
    getHeight (_,a,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,(g,h,i)) = TxSummary (fromIntegral a) (fromIntegral b) (unDbHash c) d e f g (unPgJsonb <$> h) (maybe TxFailed (const TxSucceeded) i)

getTransactionCount :: (LogLevel -> String -> IO ()) -> P.Pool Connection -> IO (Maybe Int64)
getTransactionCount logger pool = do
    P.withResource pool $ \c -> do
      runBeamPostgresDebug (logger Debug) c $ runSelectReturningOne $ select $
        aggregate_ (\_ -> as_ @Int64 countAll_) (all_ (_cddb_transactions database))

data RecentTxs = RecentTxs
  { _recentTxs_txs :: Seq TxSummary
  } deriving (Eq,Show)

getSummaries :: RecentTxs -> [TxSummary]
getSummaries (RecentTxs s) = toList s

addNewTransactions :: Seq TxSummary -> RecentTxs -> RecentTxs
addNewTransactions txs (RecentTxs s1) = RecentTxs s2
  where
    maxTransactions = 10
    s2 = S.take maxTransactions $ txs <> s1

logFunc :: ServerEnv -> Env -> LogLevel -> String -> IO ()
logFunc serverEnv env level s = if _serverEnv_verbose serverEnv then (_env_logger env) level (fromString s) else return ()

unPgJsonb :: PgJSONB a -> a
unPgJsonb (PgJSONB v) = v
