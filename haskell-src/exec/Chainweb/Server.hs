{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Server where

------------------------------------------------------------------------------
import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.ChainId
import           Chainweb.Api.Hash
import           Chainweb.Api.NodeInfo
import           Control.Applicative
import           Control.Concurrent
import           Control.Error
import           Control.Exception (bracket_)
import           Control.Monad.Except
import qualified Control.Monad.Managed as M
import           Control.Retry
import           Data.Aeson hiding (Error)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64.URL as B64
import           Data.ByteString.Lazy (ByteString)
import           Data.Decimal
import           Data.Foldable
import           Data.Int
import           Data.IORef
import qualified Data.Pool as P
import           Data.Proxy
import           Data.Sequence (Seq)
import qualified Data.Sequence as S
import           Data.String
import           Data.String.Conv (toS)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Tuple.Strict (T2(..))
import qualified Data.Vector as V
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL
import           Database.Beam.Postgres
import qualified Database.PostgreSQL.Simple.Transaction as PG
import           Control.Lens
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Network.Wai.Middleware.Cors
import           Servant.API
import           Servant.Server
import           Servant.Swagger.UI
import           System.Directory
import           System.FilePath
import           System.Logger.Types hiding (logg)
import           Text.Printf
------------------------------------------------------------------------------
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Api.StringEncoded (StringEncoded(..))
import           Chainweb.Coins
import           ChainwebDb.Database
import           ChainwebDb.Queries
import           ChainwebData.Env
import           Chainweb.Gaps
import           Chainweb.Listen
import           Chainweb.Lookups
import           Chainweb.RichList
import           ChainwebData.Types
import           ChainwebData.Api
import           ChainwebData.TransferDetail
import           ChainwebData.EventDetail
import qualified ChainwebData.Spec as Spec
import           ChainwebData.Pagination
import           ChainwebData.TxDetail
import           ChainwebData.TxSummary
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Common
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Event
import           ChainwebDb.BoundedScan
------------------------------------------------------------------------------

setCors :: Middleware
setCors = cors . const . Just $ simpleCorsResourcePolicy
    { corsRequestHeaders = simpleHeaders
    , corsExposedHeaders = Just ["Chainweb-Next"]
    }

data ServerState = ServerState
    { _ssHighestBlockHeight :: BlockHeight
    , _ssTransactionCount :: Maybe Int64
    , _ssCirculatingCoins :: Decimal
    } deriving (Eq,Show)

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
    => (Decimal -> f Decimal)
    -> ServerState -> f ServerState
ssCirculatingCoins = lens _ssCirculatingCoins setter
  where
    setter sc v = sc { _ssCirculatingCoins = v }

type RichlistEndpoint = "richlist.csv" :> Get '[PlainText] Text

type TxEndpoint = "tx" :> QueryParam "requestkey" Text :> Get '[JSON] TxDetail

type TheApi =
  ChainwebDataApi
  :<|> RichlistEndpoint

type ApiWithSwaggerUI
     = TheApi
  :<|> SwaggerSchemaUI "cwd-spec" "cwd-spec.json"

type ApiWithNoSwaggerUI
     = TheApi
  :<|> "cwd-spec" :> Get '[PlainText] Text -- Respond with 404

apiServer :: Env -> ServerEnv -> IO ()
apiServer env senv = do
  ecut <- queryCut env
  let logg = _env_logger env
  case ecut of
    Left e -> do
       logg Error "Error querying cut"
       logg Info $ fromString $ show e
    Right cutBS -> apiServerCut env senv cutBS

type ConnectionWithThrottling = (Connection, Double)

-- | Given the amount of contention on connections, calculate a factor between
-- 0 and 1 that should be used to scale down the amount of work done by request
-- handlers
throttlingFactor :: Integer -> Double
throttlingFactor load = if loadPerCap <= 1 then 1 else 1 / loadPerCap where
  -- We're arbitrarily assuming that Postgres will handle 3 concurrent requests
  -- without any slowdown
  loadPerCap = fromInteger load / 3

apiServerCut :: Env -> ServerEnv -> ByteString -> IO ()
apiServerCut env senv cutBS = do
  let curHeight = cutMaxHeight cutBS
      logg = _env_logger env
  t <- getCurrentTime
  let circulatingCoins = getCirculatingCoins (fromIntegral curHeight) t
  logg Info $ fromString $ "Total coins in circulation: " <> show circulatingCoins
  let pool = _env_dbConnPool env
  numTxs <- getTransactionCount logg pool
  ssRef <- newIORef $ ServerState 0 numTxs circulatingCoins
  logg Info $ fromString $ "Total number of transactions: " <> show numTxs
  _ <- forkIO $ scheduledUpdates env pool ssRef (_serverEnv_runFill senv) (_serverEnv_fillDelay senv)
  _ <- forkIO $ retryingListener env ssRef
  logg Info $ fromString "Starting chainweb-data server"
  throttledPool <- do
    loadedSrc <- mkLoadedSource $ M.managed $ P.withResource pool
    return $ do
      loadedRes <- loadedSrc
      load <- M.liftIO (lrLoadRef loadedRes)
      return (lrResource loadedRes, throttlingFactor load)

  let unThrottledPool = fst <$> throttledPool
  let serverApp req =
        ( ( recentTxsHandler logg unThrottledPool
            :<|> searchTxs logg throttledPool req
            :<|> evHandler logg throttledPool req
            :<|> txHandler logg unThrottledPool
            :<|> txsHandler logg unThrottledPool
            :<|> accountHandler logg throttledPool req
          )
            :<|> statsHandler ssRef
            :<|> coinsHandler ssRef
          )
          :<|> richlistHandler
  let swaggerServer = swaggerSchemaUIServer Spec.spec
      noSwaggerServer = throw404 "Swagger UI server is not enabled on this instance"
  Network.Wai.Handler.Warp.run (_serverEnv_port senv) $ setCors $ \req f ->
    if _serverEnv_serveSwaggerUi senv
      then serve (Proxy @ApiWithSwaggerUI) (serverApp req :<|> swaggerServer) req f
      else serve (Proxy @ApiWithNoSwaggerUI) (serverApp req :<|> noSwaggerServer) req f

retryingListener :: Env -> IORef ServerState -> IO ()
retryingListener env ssRef = do
  let logg = _env_logger env
      delay = 10_000_000
      policy = constantDelay delay
      check _ _ = do
        logg Warn $ fromString $ printf "Retrying node listener in %.1f seconds"
          (fromIntegral delay / 1_000_000 :: Double)
        return True
  retrying policy check $ \_ -> do
    logg Info "Starting node listener"
    listenWithHandler env $ serverHeaderHandler env ssRef

scheduledUpdates
  :: Env
  -> P.Pool Connection
  -> IORef ServerState
  -> Bool
  -> Maybe Int
  -> IO ()
scheduledUpdates env pool ssRef runFill fillDelay = forever $ do
    threadDelay (60 * 60 * 24 * micros)

    now <- getCurrentTime
    logg Info $ fromString $ show now
    logg Info "Recalculating coins in circulation:"
    height <- _ssHighestBlockHeight <$> readIORef ssRef
    let circulatingCoins = getCirculatingCoins (fromIntegral height) now
    logg Info $ fromString $ show circulatingCoins
    let f ss = (ss & ssCirculatingCoins .~ circulatingCoins, ())
    atomicModifyIORef' ssRef f

    numTxs <- getTransactionCount logg pool
    logg Info $ fromString $ "Updated number of transactions: " <> show numTxs
    let g ss = (ss & ssTransactionCount %~ (numTxs <|>), ())
    atomicModifyIORef' ssRef g

    h <- getHomeDirectory
    richList logg (h </> ".local/share") (ChainwebVersion $ _nodeInfo_chainwebVer $ _env_nodeInfo env)
    logg Info "Updated rich list"

    when runFill $ do
      logg Info "Filling missing blocks"
      gaps env (FillArgs fillDelay False)
      logg Info "Fill finished"
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
    mkStats ss = T.pack $ show $ _ssCirculatingCoins ss

statsHandler :: IORef ServerState -> Handler ChainwebDataStats
statsHandler ssRef = liftIO $ do
    fmap mkStats $ readIORef ssRef
  where
    mkStats ss = ChainwebDataStats (fromIntegral <$> _ssTransactionCount ss)
                                   (Just $ realToFrac $ _ssCirculatingCoins ss)

serverHeaderHandler :: Env -> IORef ServerState -> PowHeader -> IO ()
serverHeaderHandler env ssRef ph@(PowHeader h _) = do
  let pool = _env_dbConnPool env
  let ni = _env_nodeInfo env
  let chain = _blockHeader_chainId h
  let height = _blockHeader_height h
  let pair = T2 (_blockHeader_chainId h) (hashToDbHash $ _blockHeader_payloadHash h)
  let logg = _env_logger env
  payloadWithOutputs env pair >>= \case
    Left e -> do
      logg Error $ fromString $ printf "Couldn't fetch parent for: %s"
        (hashB64U $ _blockHeader_hash h)
      logg Info $ fromString $ show e
    Right pl -> do
      let hash = _blockHeader_hash h
          tos = _blockPayloadWithOutputs_transactionsWithOutputs pl
          ts = S.fromList $ map (\(t,tout) -> mkTxSummary chain height hash t tout) tos
          f ss = (ss & ssHighestBlockHeight %~ max height
                     & (ssTransactionCount . _Just) +~ (fromIntegral $ S.length ts), ())

      let msg = printf "Got new header on chain %d height %d" (unChainId chain) height
          addendum = if S.length ts == 0
                       then ""
                       else printf " with %d transactions" (S.length ts)

      logg Debug (fromString $ msg <> addendum)
      mapM_ (logg Debug . fromString . show) tos

      atomicModifyIORef' ssRef f
      insertNewHeader (_nodeInfo_chainwebVer ni) pool ph pl


instance BeamSqlBackendIsString Postgres (Maybe Text)
instance BeamSqlBackendIsString Postgres (Maybe String)

type TxSearchToken = BSContinuation TxCursor

readTxToken :: NextToken -> Maybe TxSearchToken
readTxToken tok = readBSToken tok <&> \mbBSC -> mbBSC <&> \(hgt, reqkey) ->
  TxCursor hgt (DbHash reqkey)

mkTxToken :: TxSearchToken -> NextToken
mkTxToken txt = mkBSToken $ txt <&> \c -> (txcHeight c, unDbHash $ txcReqKey c)

-- We're looking up the execution strategy directly inside the 'Request'
-- instead of properly adding it as a RequestHeader to the servant endpoint
-- definition, because we don't actually expect the clients to set this
-- header. This header is meant for the application gateway to set for
-- tuning purposes.
isBoundedStrategy :: Request -> Either ByteString Bool
isBoundedStrategy req =
  case lookup (fromString headerName) $ requestHeaders req of
    Nothing -> Right False
    Just header -> case header of
      "Bounded" -> Right True
      "Unbounded" -> Right False
      other -> Left $ toS $ "Unknown " <> fromString headerName <> ": " <> other
  where headerName = "Chainweb-Execution-Strategy"

isBoundedStrategyM :: Request -> Handler Bool
isBoundedStrategyM req = do
  either throw400 return $ isBoundedStrategy req

mkContinuation :: MonadError ServerError m =>
  (NextToken -> Maybe b) ->
  Maybe Offset ->
  Maybe NextToken ->
  m (Either (Maybe Integer) b)
mkContinuation readTkn mbOffset mbNext = case (mbNext, mbOffset) of
    (Just nextToken, Nothing) -> case readTkn nextToken of
      Nothing -> throw400 $ toS $ "Invalid next token: " <> unNextToken nextToken
      Just cont -> return $ Right cont
    (Just _, Just _) -> throw400 "next token query parameter not allowed with offset"
    (Nothing, Just (Offset offset)) -> return $ Left $ offset <$ guard (offset > 0)
    (Nothing, Nothing) -> return $ Left Nothing

dbToApiTxSummary :: DbTxSummary -> ContinuationHistory -> TxSummary
dbToApiTxSummary s contHist = TxSummary
  { _txSummary_chain = fromIntegral $ dtsChainId s
  , _txSummary_height = fromIntegral $ dtsHeight s
  , _txSummary_blockHash = unDbHash $ dtsBlock s
  , _txSummary_creationTime = dtsCreationTime s
  , _txSummary_requestKey = unDbHash $ dtsReqKey s
  , _txSummary_sender = dtsSender s
  , _txSummary_code = dtsCode s
  , _txSummary_continuation = unPgJsonb <$> dtsContinuation s
  , _txSummary_result = maybe TxFailed (const TxSucceeded) $ dtsGoodResult s
  , _txSummary_initialCode = chCode contHist
  , _txSummary_previousSteps = V.toList (chSteps contHist) <$ chCode contHist
  }

searchTxs
  :: LogFunctionIO Text
  -> M.Managed ConnectionWithThrottling
  -> Request
  -> Maybe Limit
  -> Maybe Offset
  -> Maybe Text
  -> Maybe BlockHeight -- ^ minimum block height
  -> Maybe BlockHeight -- ^ maximum block height
  -> Maybe NextToken
  -> Handler (NextHeaders [TxSummary])
searchTxs _ _ _ _ _ Nothing _ _ _ = throw404 "You must specify a search string"
searchTxs logger pool req givenMbLim mbOffset (Just search) minheight maxheight mbNext = do
  liftIO $ logger Info $ fromString $ printf
    "Transaction search from %s: %s" (show $ remoteHost req) (T.unpack search)
  continuation <- mkContinuation readTxToken mbOffset mbNext

  isBounded <- isBoundedStrategyM req

  liftIO $ M.with pool $ \(c, throttling) -> do
    let
      scanLimit = ceiling $ 50000 * throttling
      maxResultLimit = ceiling $ 250 * throttling
      resultLimit = min maxResultLimit $ maybe 10 unLimit givenMbLim
      strategy = if isBounded then Bounded scanLimit else Unbounded

    PG.withTransactionLevel PG.RepeatableRead c $ do
      (mbCont, results) <- performBoundedScan strategy
        (runBeamPostgresDebug (logger Debug . T.pack) c)
        (toTxSearchCursor . txwhSummary)
        (txSearchSource search $ HeightRangeParams minheight maxheight)
        noDecoration
        continuation
        resultLimit
      return $ maybe noHeader (addHeader . mkTxToken) mbCont $
        results <&> \(txwh,_) -> dbToApiTxSummary (txwhSummary txwh) (txwhContHistory txwh)

throw404 :: MonadError ServerError m => ByteString -> m a
throw404 msg = throwError $ err404 { errBody = msg }

throw400 :: MonadError ServerError m => ByteString -> m a
throw400 msg = throwError $ err400 { errBody = msg }

toApiTxDetail :: Transaction -> ContinuationHistory -> Block -> [Event] -> TxDetail
toApiTxDetail tx contHist blk evs = TxDetail
        { _txDetail_ttl = fromIntegral $ _tx_ttl tx
        , _txDetail_gasLimit = fromIntegral $ _tx_gasLimit tx
        , _txDetail_gasPrice = _tx_gasPrice tx
        , _txDetail_nonce = _tx_nonce tx
        , _txDetail_pactId = unDbHash <$> _tx_pactId tx
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
        , _txDetail_requestKey = unDbHash $ _tx_requestKey tx
        , _txDetail_sender = _tx_sender tx
        , _txDetail_code = _tx_code tx
        , _txDetail_success = isJust $ _tx_goodResult tx
        , _txDetail_events = map toTxEvent evs
        , _txDetail_initialCode = chCode contHist
        , _txDetail_previousSteps = V.toList (chSteps contHist) <$ chCode contHist
        }
  where
    unMaybeValue = maybe Null unPgJsonb
    toTxEvent ev =
      TxEvent (_ev_qualName ev) (unPgJsonb $ _ev_params ev)

queryTxsByKey :: LogFunctionIO Text -> Text -> Connection -> IO [TxDetail]
queryTxsByKey logger rk c =
  runBeamPostgresDebug (logger Debug . T.pack) c $ do
    r <- runSelectReturningList $ select $ do
      tx <- all_ (_cddb_transactions database)
      contHist <- joinContinuationHistory (_tx_pactId tx)
      blk <- all_ (_cddb_blocks database)
      guard_ (_tx_block tx `references_` blk)
      guard_ (_tx_requestKey tx ==. val_ (DbHash rk))
      return (tx,contHist, blk)
    evs <- runSelectReturningList $ select $ do
       ev <- all_ (_cddb_events database)
       guard_ (_ev_requestkey ev ==. val_ (RKCB_RequestKey $ DbHash rk))
       return ev
    return $ (`fmap` r) $ \(tx,contHist, blk) -> toApiTxDetail tx contHist blk evs

txHandler
  :: LogFunctionIO Text
  -> M.Managed Connection
  -> Maybe RequestKey
  -> Handler TxDetail
txHandler _ _ Nothing = throw404 "You must specify a search string"
txHandler logger pool (Just (RequestKey rk)) =
  liftIO (M.with pool $ queryTxsByKey logger rk) >>= \case
    [x] -> return x
    _ -> throw404 "Tx not found"

txsHandler
  :: LogFunctionIO Text
  -> M.Managed Connection
  -> Maybe RequestKey
  -> Handler [TxDetail]
txsHandler _ _ Nothing = throw404 "You must specify a search string"
txsHandler logger pool (Just (RequestKey rk)) =
  liftIO (M.with pool $ queryTxsByKey logger rk) >>= \case
    [] -> throw404 "Tx not found"
    xs -> return xs

type AccountNextToken = (Int64, T.Text, Int64)

readToken :: Read a => NextToken -> Maybe a
readToken (NextToken nextToken) = readMay $ toS $ B64.decodeLenient $ toS nextToken

mkToken :: Show a => a -> NextToken
mkToken contents = NextToken $ T.pack $
  toS $ BS.filter (/= 0x3d) $ B64.encode $ toS $ show contents

accountHandler
  :: LogFunctionIO Text
  -> M.Managed ConnectionWithThrottling
  -> Request
  -> Text -- ^ account identifier
  -> Maybe Text -- ^ token type
  -> Maybe ChainId -- ^ chain identifier
  -> Maybe BlockHeight -- ^ minimum block height
  -> Maybe BlockHeight -- ^ maximum block height
  -> Maybe Limit
  -> Maybe Offset
  -> Maybe NextToken
  -> Handler (NextHeaders [TransferDetail])
accountHandler logger pool req account token chain minheight maxheight limit mbOffset mbNext = do
  let usedCoinType = fromMaybe "coin" token
  liftIO $ logger Info $
    fromString $ printf "Account search from %s for: %s %s %s" (show $ remoteHost req) (T.unpack account) (T.unpack usedCoinType) (maybe "<all-chains>" show chain)

  continuation <- mkContinuation readEventToken mbOffset mbNext
  isBounded <- isBoundedStrategyM req
  let searchParams = TransferSearchParams
       { tspToken = usedCoinType
       , tspChainId = chain
       , tspHeightRange = HeightRangeParams minheight maxheight
       , tspAccount = account
       }
  liftIO $ M.with pool $ \(c, throttling) -> do
    let
      scanLimit = ceiling $ 50000 * throttling
      maxResultLimit = ceiling $ 250 * throttling
      resultLimit = min maxResultLimit $ maybe 10 unLimit limit
      strategy = if isBounded then Bounded scanLimit else Unbounded
    PG.withTransactionLevel PG.RepeatableRead c $ do
      (mbCont, results) <- performBoundedScan strategy
        (runBeamPostgresDebug (logger Debug . T.pack) c)
        toAccountsSearchCursor
        (transfersSearchSource searchParams)
        transferSearchExtras
        continuation
        resultLimit
      return $ maybe noHeader (addHeader . mkEventToken) mbCont  $ results <&> \(tr, extras) -> TransferDetail
        { _trDetail_token = _tr_modulename tr
        , _trDetail_chain = fromIntegral $ _tr_chainid tr
        , _trDetail_height = fromIntegral $ _tr_height tr
        , _trDetail_blockHash = unDbHash $ unBlockId $ _tr_block tr
        , _trDetail_requestKey = getTxHash $ _tr_requestkey tr
        , _trDetail_idx = fromIntegral $ _tr_idx tr
        , _trDetail_amount = StringEncoded $ getKDAScientific $ _tr_amount tr
        , _trDetail_fromAccount = _tr_from_acct tr
        , _trDetail_toAccount = _tr_to_acct tr
        , _trDetail_crossChainAccount = tseXChainAccount extras
        , _trDetail_crossChainId = fromIntegral <$> tseXChainId extras
        , _trDetail_blockTime = tseBlockTime extras
        }

type EventSearchToken = BSContinuation EventCursor

readBSToken :: Read cursor => NextToken -> Maybe (BSContinuation cursor)
readBSToken tok = readToken tok <&> \(cursor, offNum) ->
  BSContinuation cursor $ if offNum <= 0 then Nothing else Just offNum

mkBSToken :: Show cursor => BSContinuation cursor -> NextToken
mkBSToken (BSContinuation cur mbOff) = mkToken (cur, fromMaybe 0 mbOff)

readEventToken :: NextToken -> Maybe EventSearchToken
readEventToken tok = readBSToken tok <&> \mbBSC -> mbBSC <&> \(hgt,reqkey,idx) ->
  EventCursor hgt (rkcbFromText reqkey) idx

mkEventToken :: EventSearchToken -> NextToken
mkEventToken est = mkBSToken $ est <&> \c ->
  ( ecHeight c
  , show $ ecReqKey c
  , ecIdx c
  )

evHandler
  :: LogFunctionIO Text
  -> M.Managed ConnectionWithThrottling
  -> Request
  -> Maybe Limit
  -> Maybe Offset
  -> Maybe Text -- ^ fulltext search
  -> Maybe EventParam
  -> Maybe EventName
  -> Maybe EventModuleName
  -> Maybe BlockHeight -- ^ minimum block height
  -> Maybe BlockHeight -- ^ maximum block height
  -> Maybe NextToken
  -> Handler (NextHeaders [EventDetail])
evHandler logger pool req limit mbOffset qSearch qParam qName qModuleName minheight maxheight mbNext = do
  liftIO $ logger Info $ fromString $ printf "Event search from %s: %s" (show $ remoteHost req) (maybe "\"\"" T.unpack qSearch)
  continuation <- mkContinuation readEventToken mbOffset mbNext
  let searchParams = EventSearchParams
        { espSearch = qSearch
        , espParam = qParam
        , espName = qName
        , espModuleName = qModuleName
        }

  isBounded <- isBoundedStrategyM req

  liftIO $ M.with pool $ \(c, throttling) -> do
    let
      scanLimit = ceiling $ 50000 * throttling
      maxResultLimit = ceiling $ 250 * throttling
      resultLimit = min maxResultLimit $ maybe 10 unLimit limit
      strategy = if isBounded then Bounded scanLimit else Unbounded
    PG.withTransactionLevel PG.RepeatableRead c $ do
      (mbCont, results) <- performBoundedScan strategy
        (runBeamPostgresDebug (logger Debug . T.pack) c)
        toEventsSearchCursor
        (eventsSearchSource searchParams $ HeightRangeParams minheight maxheight)
        eventSearchExtras
        continuation
        resultLimit
      return $ maybe noHeader (addHeader . mkEventToken) mbCont $
        results <&> \(ev,extras) -> EventDetail
          { _evDetail_name = _ev_qualName ev
          , _evDetail_params = unPgJsonb $ _ev_params ev
          , _evDetail_moduleHash = _ev_moduleHash ev
          , _evDetail_chain = fromIntegral $ _ev_chainid ev
          , _evDetail_height = fromIntegral $ _ev_height ev
          , _evDetail_blockTime = eseBlockTime extras
          , _evDetail_blockHash = unDbHash $ unBlockId $ _ev_block ev
          , _evDetail_requestKey = getTxHash $ _ev_requestkey ev
          , _evDetail_idx = fromIntegral $ _ev_idx ev
          }

data h :. t = h :. t deriving (Eq,Ord,Show,Read,Typeable)
infixr 3 :.

type instance QExprToIdentity (a :. b) = (QExprToIdentity a) :. (QExprToIdentity b)
type instance QExprToField (a :. b) = (QExprToField a) :. (QExprToField b)


recentTxsHandler :: LogFunctionIO Text -> M.Managed Connection ->  Handler [TxSummary]
recentTxsHandler logger pool = liftIO $ do
    logger Info "Getting recent transactions"
    M.with pool $ \c -> do
      res <- runBeamPostgresDebug (logger Debug . T.pack) c $
        runSelectReturningList $ select $ do
        limit_ 10 $ orderBy_ (desc_ . dtsHeight . fst) $ do
          tx <- all_ (_cddb_transactions database)
          contHist <- joinContinuationHistory (_tx_pactId tx)
          return $ (toDbTxSummary tx, contHist)
      return $ uncurry dbToApiTxSummary <$> res

getTransactionCount :: LogFunctionIO Text -> P.Pool Connection -> IO (Maybe Int64)
getTransactionCount logger pool = do
    P.withResource pool $ \c -> do
      runBeamPostgresDebug (logger Debug . T.pack) c $ runSelectReturningOne $ select $
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

unPgJsonb :: PgJSONB a -> a
unPgJsonb (PgJSONB v) = v

-- | A "LoadedResource" is a resource along with a way to read an integer
-- quantity representing how much load there is on the resource currently
data LoadedResource resource = LoadedResource
  { lrResource :: resource
  , lrLoadRef :: IO Integer
  }

type LoadedSource resource = M.Managed (LoadedResource resource)

-- | Wrap a given "Managed" with a layer that keeps track of how many other
-- consumers there currently are using or waiting on the inner "Managed".
-- At any given moment, this number can be read through the "lrLoadRef" of the
-- provided "LoadedResource".
mkLoadedSource :: M.Managed resource -> IO (M.Managed (LoadedResource resource))
mkLoadedSource innerSource = do
  loadRef <- newIORef 0
  let modifyLoad f = atomicModifyIORef' loadRef $ \load -> (f load, ())
  return $ M.managed $ \outerBorrower ->
    bracket_ (modifyLoad succ) (modifyLoad pred) $
      M.with innerSource $ \resource -> outerBorrower $
        LoadedResource resource (readIORef loadRef)
