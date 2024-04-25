{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeApplications #-}

module Chainweb.Lookups
  ( -- * Endpoints
    blocksBetween
  , payloadWithOutputs
  , getNodeInfo
  , queryCut
  , cutMaxHeight
    -- * Transformations
  , mkBlockTransactions
  , mkBlockEvents
  , mkBlockEvents'
  , mkBlockEventsWithCreationTime
  , mkCoinbaseEvents
  , mkTransactionSigners
  , mkTransactionVerifiers
  , mkTransferRows
  , bpwoMinerKeys

  , ErrorType(..)
  , ApiError(..)
  , handleRequest
  ) where

import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BlockPayloadWithOutputs
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.ChainwebMeta
import           Chainweb.Api.Hash
import           Chainweb.Api.MinerData
import           Chainweb.Api.NodeInfo
import           Chainweb.Api.PactCommand
import           Chainweb.Api.Payload
import           Chainweb.Api.Sig
import qualified Chainweb.Api.Verifier as CW
import qualified Chainweb.Api.Signer as CW
import qualified Chainweb.Api.Transaction as CW
import           ChainwebData.Env
import           ChainwebData.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Common
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Event
import           ChainwebDb.Types.Signer
import           ChainwebDb.Types.Transaction
import           ChainwebDb.Types.Transfer
import           ChainwebDb.Types.Verifier
import           Control.Applicative
import           Control.Lens
import           Control.Monad
import           Data.Aeson
import qualified Data.Aeson.KeyMap as KeyMap
import           Data.Aeson.Lens
import           Data.Aeson.Types
import           Data.ByteString.Lazy (ByteString,toStrict)
import           Data.Foldable
import           Data.Int
import           Data.Maybe
import           Data.String (fromString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Read as TR
import           Data.Time.Clock (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Tuple.Strict (T2(..))
import           Database.Beam hiding (insert)
import           Database.Beam.Postgres
import           Network.HTTP.Client hiding (Proxy)
import           Network.HTTP.Types
import qualified System.Logger.Types as Logger
import           Text.Printf

data ErrorType = RateLimiting | ClientError | ServerError | OtherError T.Text
  deriving (Eq,Ord,Show)

data ApiError = ApiError
  { apiError_type :: ErrorType
  , apiError_status :: Status
  , apiError_body :: ByteString
  } deriving (Eq,Ord,Show)

handleRequest :: Request -> Manager -> IO (Either ApiError (Response ByteString))
handleRequest req mgr = do
  res <- httpLbs req mgr
  let mkErr t = ApiError t (responseStatus res) (responseBody res)
      checkErr s
        | statusCode s == 429 || statusCode s == 403 = Left $ mkErr RateLimiting
        | statusIsClientError s = Left $ mkErr ClientError
        | statusIsServerError s = Left $ mkErr ServerError
        | statusIsSuccessful s = Right res
        | otherwise = Left $ mkErr $ OtherError "unknown error"
  pure $ checkErr (responseStatus res)

--------------------------------------------------------------------------------
-- Endpoints
--
-- | Returns headers in the range [low, high] (inclusive).
blocksBetween
  :: Env
  -> (ChainId, Low, High)
  -> IO (Either ApiError [(BlockHeader, BlockPayloadWithOutputs)])
blocksBetween env (cid, Low low, High up) = do
  req <- parseRequest url
  eresp <- handleRequest (req { requestHeaders = requestHeaders req <> encoding })
                     (_env_httpManager env)
  let
    logg = _env_logger env
  case eresp of
    Left e -> pure $ Left e
    Right resp -> do
      let
        responseJson = eitherDecode' (responseBody resp)
        responseParser =
          withObject "BlocksOutput" $ \o ->
            o .: "items"
      items <- case parseEither responseParser =<< responseJson  of
        Left e -> do
          logg Logger.Error $ fromString $ printf "Error parsing blocksBetween response: %s" e
          return []
        Right items -> pure items
      fmap (Right . catMaybes) $ forM (zip items [0 :: Int ..]) $ \(item, idx) -> do
        let
          itemParser = withObject "Block" $ \o -> do
            (,) <$> o .: "header" <*> o .: "payloadWithOutputs"
        case parseEither itemParser item of
          Left e -> Nothing <$ logg Logger.Error
            (fromString $ printf "Error parsing block %d in range %s: %s" idx rangeStr e)
            where rangeStr = show (cid, low, up)
          Right res -> pure $ Just res
  where
    v = _nodeInfo_chainwebVer $ _env_nodeInfo env
    url = showUrlScheme (_env_serviceUrlScheme env) <> query
    query = printf "/chainweb/0.0/%s/chain/%d/block?minheight=%d&maxheight=%d"
      (T.unpack v) (unChainId cid) low up
    encoding = [("accept", "application/json")]

payloadWithOutputs
  :: Env
  -> T2 ChainId (DbHash PayloadHash)
  -> IO (Either ApiError BlockPayloadWithOutputs)
payloadWithOutputs env (T2 cid0 hsh0) = do
  req <- parseRequest url
  eresp <- handleRequest req (_env_httpManager env)
  let res = do
        resp <- eresp
        case eitherDecode' (responseBody resp) of
          Left e -> Left $ ApiError (OtherError $ "Decoding error in payloadWithOutputs: " <> T.pack e)
                                    (responseStatus resp) (responseBody resp)
          Right a -> Right a
  pure res
  where
    v = _nodeInfo_chainwebVer $ _env_nodeInfo env
    url = showUrlScheme (_env_serviceUrlScheme env) <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> cid <> "/payload/" <> hsh <> "/outputs"
    cid = T.pack $ show cid0
    hsh = unDbHash hsh0

-- | Query a node for the `ChainId` values its current `ChainwebVersion` has
-- available.
getNodeInfo :: Manager -> UrlScheme -> IO (Either String NodeInfo)
getNodeInfo m us = do
  req <- parseRequest $ showUrlScheme us <> "/info"
  res <- httpLbs req m
  pure $ eitherDecode' (responseBody res)

queryCut :: Env -> IO (Either ApiError ByteString)
queryCut e = do
  let v = _nodeInfo_chainwebVer $ _env_nodeInfo e
      m = _env_httpManager e
      u = _env_serviceUrlScheme e
      url = printf "%s/chainweb/0.0/%s/cut" (showUrlScheme u) (T.unpack v)
  req <- parseRequest url
  res <- handleRequest req m
  pure $ responseBody <$> res

cutMaxHeight :: ByteString -> Integer
cutMaxHeight bs = maximum $ (0:) $ bs ^.. key "hashes" . members . key "height" . _Integer


--------------------------------------------------------------------------------
-- Transformations

-- | Derive useful database entries from a `Block` and its payload.
mkBlockTransactions :: Block -> BlockPayloadWithOutputs -> [Transaction]
mkBlockTransactions b pl = map (mkTransaction b) $ _blockPayloadWithOutputs_transactionsWithOutputs pl

{- ¡ARRIBA!-}
-- The blockhash is the hash of the current block. A Coinbase transaction's
-- request key is expected to the parent hash of the block it is found in.
-- However, the source key of the event in chainweb-data database instance is
-- the current block hash and NOT the parent hash However, the source key of the
-- event in chainweb-data database instance is the current block hash and NOT
-- the parent hash.
mkBlockEvents' :: Int64 -> ChainId -> DbHash BlockHash -> BlockPayloadWithOutputs -> ([Event], [(DbHash TxHash, [Event])])
mkBlockEvents' height cid blockhash pl =
    (mkCoinbaseEvents height cid blockhash pl, map mkPair tos)
  where
    tos = _blockPayloadWithOutputs_transactionsWithOutputs pl
    mkPair p = ( DbHash $ hashB64U $ CW._transaction_hash $ fst p
               , mkTxEvents height cid blockhash p)

mkBlockEventsWithCreationTime :: Int64 -> ChainId -> DbHash BlockHash -> BlockPayloadWithOutputs -> ([Event], [(DbHash TxHash, UTCTime, [Event])])
mkBlockEventsWithCreationTime height cid blockhash pl = (mkCoinbaseEvents height cid blockhash pl, map mkTriple tos)
  where
    tos = _blockPayloadWithOutputs_transactionsWithOutputs pl
    mkTriple p = (DbHash $ hashB64U $ CW._transaction_hash $ fst p
                 , posixSecondsToUTCTime $ _chainwebMeta_creationTime $ _pactCommand_meta $ CW._transaction_cmd $ fst p
                 , mkTxEvents height cid blockhash p)

mkBlockEvents :: Int64 -> ChainId -> DbHash BlockHash -> BlockPayloadWithOutputs -> [Event]
mkBlockEvents height cid blockhash pl =  cbes ++ concatMap snd txes
  where
    (cbes, txes) = mkBlockEvents' height cid blockhash pl

mkTransferRows :: Int64 -> ChainId -> DbHash BlockHash -> UTCTime -> BlockPayloadWithOutputs -> Int -> [Transfer]
mkTransferRows height cid@(ChainId cid') blockhash _creationTime pl eventMinHeight =
    let (coinbaseEvs, evs) = mkBlockEventsWithCreationTime height cid blockhash pl
    in if height >= fromIntegral eventMinHeight
          then createNonCoinBaseTransfers evs ++ createCoinBaseTransfers coinbaseEvs
          else []
  where
    unwrap (PgJSONB a) = a
    mkTransfer mReqKey ev = do
      let (PgJSONB params) = _ev_params ev
      amount <- getAmount params
      fromAccount <- params ^? ix 0 . _String
      toAccount <- params ^? ix 1 . _String
      return Transfer
        {
          _tr_block = BlockId blockhash
        , _tr_requestkey = maybe RKCB_Coinbase RKCB_RequestKey mReqKey
        , _tr_chainid = fromIntegral cid'
        , _tr_height = height
        , _tr_idx = _ev_idx ev
        , _tr_modulename = _ev_module ev
        , _tr_moduleHash = _ev_moduleHash ev
        , _tr_from_acct = fromAccount
        , _tr_to_acct = toAccount
        , _tr_amount = amount
        }
    getAmount :: [Value] -> Maybe KDAScientific
    getAmount params = fmap KDAScientific $
        (params ^? ix 2 . key "decimal" . _Number)
        <|>
        (params ^? ix 2 . key "decimal" . _String . to TR.rational  . _Right . _1)
        <|>
        (params ^? ix 2 . key "int" . _Number)
        <|>
        (params ^? ix 2 . key "int" . _String . to TR.rational . _Right . _1)
        <|>
        (params ^? ix 2 . _Number)
        <|>
        (params ^? ix 2 . _String . to TR.rational . _Right . _1)
    createCoinBaseTransfers = fmap (fromMaybe (error "<impossible>") . mkTransfer Nothing)
    createNonCoinBaseTransfers xs = [ transfer
      | (txhash,_,evs) <- xs
      , ev <- evs
      , T.takeEnd 8 (_ev_qualName ev) == "TRANSFER"
      , length (unwrap (_ev_params ev)) == 3
      , transfer <- maybeToList $ mkTransfer (Just txhash) ev
      ]

mkTransactionSigners :: CW.Transaction -> [Signer]
mkTransactionSigners t = zipWith3 mkSigner signers sigs [0..]
  where
    signers = _pactCommand_signers $ CW._transaction_cmd t
    sigs = CW._transaction_sigs t
    mkSigner signer sig idx = Signer
      (DbHash $ hashB64U $ CW._transaction_hash t)
      idx
      (CW._signer_pubKey signer)
      (CW._signer_scheme signer)
      (CW._signer_addr signer)
      (PgJSONB $ map toJSON $ CW._signer_capList signer)
      (Signature $ unSig sig)

mkTransactionVerifiers :: CW.Transaction -> [Verifier]
mkTransactionVerifiers t = maybe [] (zipWith mkVerifier [0..]) verifiers
  where
    verifiers :: Maybe [CW.Verifier]
    verifiers = _pactCommand_verifiers $ CW._transaction_cmd t
    requestkey = CW._transaction_hash t
    mkVerifier idx verifier = Verifier
      { _verifier_requestkey = DbHash $ hashB64U requestkey
      , _verifier_idx = idx
      , _verifier_name = CW._verifier_name verifier
      , _verifier_proof = (CW._verifier_proof verifier) ^? _String
      , _verifier_caps = PgJSONB $ map toJSON $ CW._verifier_capList verifier
      }

mkCoinbaseEvents :: Int64 -> ChainId -> DbHash BlockHash -> BlockPayloadWithOutputs -> [Event]
mkCoinbaseEvents height cid blockhash pl = _blockPayloadWithOutputs_coinbase pl
    & coinbaseTO
    & _toutEvents
    {- idx of coinbase transactions is set to 0.... this value is just a placeholder-}
    <&> \ev -> mkEvent cid height blockhash Nothing ev 0
  where
    coinbaseTO (Coinbase t) = t

bpwoMinerKeys :: BlockPayloadWithOutputs -> [T.Text]
bpwoMinerKeys = _minerData_publicKeys . _blockPayloadWithOutputs_minerData

mkTransaction :: Block -> (CW.Transaction, TransactionOutput) -> Transaction
mkTransaction b (tx,txo) = Transaction
  { _tx_requestKey = DbHash $ hashB64U $ CW._transaction_hash tx
  , _tx_block = pk b
  , _tx_chainId = _block_chainId b
  , _tx_height = _block_height b
  , _tx_creationTime = posixSecondsToUTCTime $ _chainwebMeta_creationTime mta
  , _tx_ttl = fromIntegral $ _chainwebMeta_ttl mta
  , _tx_gasLimit = fromIntegral $ _chainwebMeta_gasLimit mta
  , _tx_gasPrice = realToFrac $ _chainwebMeta_gasPrice mta
  , _tx_sender = _chainwebMeta_sender mta
  , _tx_nonce = _pactCommand_nonce cmd
  , _tx_code = _exec_code <$> exc
  , _tx_pactId = DbHash . _cont_pactId <$> cnt
  , _tx_rollback = _cont_rollback <$> cnt
  , _tx_step = fromIntegral . _cont_step <$> cnt
  , _tx_data = (PgJSONB . _cont_data <$> cnt)
    <|> (PgJSONB <$> (exc >>= _exec_data))
  , _tx_proof = join (_cont_proof <$> cnt)

  , _tx_gas = fromIntegral $ _toutGas txo
  , _tx_badResult = badres
  , _tx_goodResult = goodres
  , _tx_logs = hashB64U <$> _toutLogs txo
  , _tx_metadata = PgJSONB <$> _toutMetaData txo
  , _tx_continuation = PgJSONB <$> _toutContinuation txo
  , _tx_txid = fromIntegral <$> _toutTxId txo
  , _tx_numEvents = Just $ fromIntegral $ length $ _toutEvents txo
  }
  where
    cmd = CW._transaction_cmd tx
    mta = _pactCommand_meta cmd
    pay = _pactCommand_payload cmd
    exc = case pay of
      ExecPayload e -> Just e
      ContPayload _ -> Nothing
    cnt = case pay of
      ExecPayload _ -> Nothing
      ContPayload c -> Just c
    (badres, goodres) = case _toutResult txo of
      PactResult (Left v) -> (Just $ PgJSONB v, Nothing)
      PactResult (Right v) -> (Nothing, Just $ PgJSONB v)

mkTxEvents :: Int64 -> ChainId -> DbHash BlockHash -> (CW.Transaction,TransactionOutput) -> [Event]
mkTxEvents height cid blk (tx,txo) = zipWith (mkEvent cid height blk (Just rk)) (_toutEvents txo) [0..]
  where
    rk = DbHash $ hashB64U $ CW._transaction_hash tx


mkEvent :: ChainId -> Int64 -> DbHash BlockHash -> Maybe (DbHash TxHash) -> Value -> Int64 -> Event
mkEvent (ChainId chainid) height block requestkey ev idx = Event
    { _ev_requestkey = maybe RKCB_Coinbase RKCB_RequestKey requestkey
    , _ev_block = BlockId block
    , _ev_chainid = fromIntegral chainid
    , _ev_height = height
    , _ev_idx = idx
    , _ev_name = ename ev
    , _ev_qualName = qname ev
    , _ev_module = emodule ev
    , _ev_moduleHash = emoduleHash ev
    , _ev_paramText = T.decodeUtf8 $ toStrict $ encode $ params ev
    , _ev_params = PgJSONB $ toList $ params ev
    }
  where
    ename = fromMaybe "" . str "name"
    emodule = fromMaybe "" . join . fmap qualm . lkp "module"
    qname ev' = case join $ fmap qualm $ lkp "module" ev' of
      Nothing -> ename ev'
      Just m -> m <> "." <> ename ev'
    qualm v = case str "namespace" v of
      Nothing -> mn
      Just n -> ((n <> ".") <>) <$> mn
      where mn = str "name" v
    emoduleHash = fromMaybe "" . str "moduleHash"
    params = fromMaybe mempty . fmap ar . lkp "params"
    ar v = case v of
      Array l -> l
      _ -> mempty
    lkp n v = case v of
      Object o -> KeyMap.lookup n o
      _ -> Nothing
    str n v = case lkp n v of
      Just (String s) -> Just s
      _ -> Nothing
