{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           Chainweb.Backfill (backfill)
import           Chainweb.BackfillTransfers (backfillTransfersCut)
import           ChainwebDb.Database (initializeTables)
import           ChainwebData.Env
import           Chainweb.FillEvents (fillEvents)
import           Chainweb.Gaps
import           Chainweb.Listen (listen)
import           Chainweb.Lookups (getNodeInfo)
import           Chainweb.RichList (richList)
import           Chainweb.Server (apiServer)
import           Chainweb.Single (single)
import           Control.Lens
import           Control.Monad (unless,void)
import           Data.Bifunctor
import qualified Data.Pool as P
import           Data.String
import           Data.Text (Text)
import           Database.PostgreSQL.Simple
import qualified Database.PostgreSQL.Simple.Migration as Mg
import           Network.Connection hiding (Connection)
import           Network.HTTP.Client hiding (withConnection)
import           Network.HTTP.Client.TLS
import           Options.Applicative
import           System.Directory
import           System.Exit
import           System.Logger hiding (logg)
import           System.FilePath
import           Text.Printf

---

main :: IO ()
main = do
  args <- execParser opts
  withHandleBackend backendConfig $ \backend ->
    withLogger (config (getLevel args)) backend $ \logger -> do
      let logg = loggerFunIO logger
      case args of
        RichListArgs (NodeDbPath mfp) _ version -> do
          fp <- case mfp of
            Nothing -> do
              h <- getHomeDirectory
              let h' = h </> ".local/share"
              logg Info $ "Constructing rich list using default db-path: " <> fromString h'
              return h'
            Just fp -> do
              logg Info $ "Constructing rich list using given db-path: " <> fromString fp
              return fp
          richList logg fp version
        Args c pgc us u _ ms -> do
          logg Info $ "Using database: " <> fromString (show pgc)
          logg Info $ "Service API: " <> fromString (showUrlScheme us)
          logg Info $ "P2P API: " <> fromString (showUrlScheme (UrlScheme Https u))
          withPool pgc $ \pool -> do
            P.withResource pool $ \conn ->
              unless (isIndexedDisabled c) $ do
                initializeTables logg ms conn
                addTransactionsHeightIndex logg conn
                addEventsHeightChainIdIdxIndex logg conn
                addEventsHeightNameParamsIndex logg conn
                addFromAccountsIndex logg conn
                addToAccountsIndex logg conn
                addTransactionsRequestKeyIndex logg conn
                addEventsRequestKeyIndex logg conn
                initializePGSimpleMigrations logg conn
            logg Info "DB Tables Initialized"
            let mgrSettings = mkManagerSettings (TLSSettingsSimple True False False) Nothing
            m <- newManager mgrSettings
            getNodeInfo m us >>= \case
              Left e -> logg Error (fromString $ printf "Unable to connect to %s /info endpoint%s" (showUrlScheme us) e) >> exitFailure
              Right ni -> do
                let !mcids = map (second (map (ChainId . fst))) <$> _nodeInfo_graphs ni
                case mcids of
                  Nothing -> logg Error "Node did not have graph information" >> exitFailure
                  Just cids -> do
                    let !env = Env m pool us u ni cids logg
                    case c of
                      Listen -> listen env
                      Backfill as -> backfill env as
                      BackFillTransfers indexP as -> backfillTransfersCut env indexP as
                      Fill as -> gaps env as
                      Single cid h -> single env cid h
                      FillEvents as et -> fillEvents env as et
                      Server serverEnv -> apiServer env serverEnv
  where
    opts = info ((richListP <|> envP) <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
    config level = defaultLoggerConfig
      & loggerConfigThreshold .~ level
    backendConfig = defaultHandleBackendConfig
    isIndexedDisabled = \case
      Fill (FillArgs _ p) -> p
      _ -> False
    getLevel = \case
      Args _ _ _ _ level _ -> level
      RichListArgs _ level _ -> level


data IndexCreationInfo = IndexCreationInfo
  {
    message :: Text
  , statement :: Query
  }

addIndex :: IndexCreationInfo -> LogFunctionIO Text -> Connection -> IO ()
addIndex (IndexCreationInfo m s) logg conn = do
  logg Info m
  void $ execute_ conn s

addTransactionsHeightIndex :: LogFunctionIO Text -> Connection -> IO ()
addTransactionsHeightIndex =
  addIndex $
   IndexCreationInfo
    {
      message = "Adding height index on transactions table"
    , statement = "CREATE INDEX IF NOT EXISTS transactions_height_idx ON transactions(height);"
    }

addEventsHeightChainIdIdxIndex :: LogFunctionIO Text -> Connection -> IO ()
addEventsHeightChainIdIdxIndex =
  addIndex $
    IndexCreationInfo
    {
      message = "Adding (height, chainid, idx) index on events table"
    , statement = "CREATE INDEX IF NOT EXISTS events_height_chainid_idx ON events(height DESC, chainid ASC, idx ASC);"
    }

-- this is roughly "events_height_name_expr_expr1_idx" btree (height, name,
-- (params ->> 0), (params ->> 1)) WHERE name::text = 'TRANSFER'::text

addEventsHeightNameParamsIndex :: LogFunctionIO Text -> Connection -> IO ()
addEventsHeightNameParamsIndex =
  addIndex $
    IndexCreationInfo
    {
      message = "Adding \"(height,name,(params ->> 0),(params ->> 1)) WHERE name = 'TRANSFER'\" index"
    , statement = "CREATE INDEX IF NOT EXISTS events_height_name_expr_expr1_idx ON events (height desc, name, (params ->> 0), (params ->> 1)) WHERE name = 'TRANSFER';"
    }

addEventsModuleNameIndex :: LogFunctionIO Text -> Connection -> IO ()
addEventsModuleNameIndex =
  addIndex $
    IndexCreationInfo
    {
      message = "Adding \"(height desc, chainid, module)\" index"
    , statement = "CREATE INDEX IF NOT EXISTS events_height_chainid_module ON events (height DESC, chainid, module);"
    }

addFromAccountsIndex :: LogFunctionIO Text -> Connection -> IO ()
addFromAccountsIndex =
  addIndex
    IndexCreationInfo
    {
      message = "Adding \"(from_acct, height desc, idx)\" index on transfers table"
    , statement = "CREATE INDEX IF NOT EXISTS transfers_from_acct_height_idx ON transfers (from_acct, height desc, idx);"
    }

addToAccountsIndex :: LogFunctionIO Text -> Connection -> IO ()
addToAccountsIndex =
  addIndex
    IndexCreationInfo
    {
      message = "Adding \"(to_acct, height desc,idx)\" index on transfers table"
    , statement = "CREATE INDEX IF NOT EXISTS transfers_to_acct_height_idx_idx ON transfers (to_acct, height desc, idx);"
    }

addTransactionsRequestKeyIndex :: LogFunctionIO Text -> Connection -> IO ()
addTransactionsRequestKeyIndex =
  addIndex
    IndexCreationInfo
    {
      message = "Adding \"(requestkey)\" index on transactions table"
    , statement = "CREATE INDEX IF NOT EXISTS transactions_requestkey_idx ON transactions (requestkey);"
    }

addEventsRequestKeyIndex :: LogFunctionIO Text -> Connection -> IO ()
addEventsRequestKeyIndex =
  addIndex
    IndexCreationInfo
    {
      message = "Adding \"(requestkey)\" index on events table"
    , statement = "CREATE INDEX IF NOT EXISTS events_requestkey_idx ON events (requestkey);"
    }

initializePGSimpleMigrations :: LogFunctionIO Text -> Connection -> IO ()
initializePGSimpleMigrations logg conn = do
  logg Info "Initializing the incremental migrations table"
  Mg.runMigration (Mg.MigrationContext Mg.MigrationInitialization False conn) >>= \case
    Mg.MigrationError err -> do
      let msg = "Error initializing migrations: " ++ err
      logg Error $ fromString msg
      die msg
    Mg.MigrationSuccess -> logg Info "Initialized migrations"

{-
λ> :main single --chain 2 --height 1487570 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main single --chain 0 --height 1494311 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main server --port 9999 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
-}
