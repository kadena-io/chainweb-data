{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           Chainweb.Backfill (backfill)
import           Chainweb.BackfillTransfers (backfillTransfersCut)
import           ChainwebDb.Database (checkTables)
import           ChainwebDb.Migration (MigrationAction(..))
import qualified ChainwebDb.Migration as Mg

import           ChainwebData.Env
import           Chainweb.FillEvents (fillEvents)
import           Chainweb.Gaps
import           Chainweb.Listen (listen)
import           Chainweb.Lookups (getNodeInfo)
import           Chainweb.RichList (richList)
import           Chainweb.Server (apiServer)
import           Chainweb.Single (single)
import           Control.Lens
import           Control.Monad (void)
import           Data.Bifunctor
import qualified Data.ByteString as BS
import           Data.FileEmbed
import qualified Data.Pool as P
import           Data.String
import           Data.Text (Text)
import           Database.PostgreSQL.Simple
import qualified Database.PostgreSQL.Simple.Types as PG
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
        MigrateOnly pgc _ mbMigFolder -> withPool pgc $ \pool ->
          runMigrations pool logg RunMigrations mbMigFolder
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
        Args c pgc us u _ ms mbMigFolder -> do
          logg Info $ "Using database: " <> fromString (show pgc)
          logg Info $ "Service API: " <> fromString (showUrlScheme us)
          logg Info $ "P2P API: " <> fromString (showUrlScheme (UrlScheme Https u))
          withCWDPool pgc $ \pool -> do
            runMigrations pool logg ms mbMigFolder
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
                      Worker workerEnv -> apiServer env workerEnv
        CheckSchema pgc _ -> withCWDPool pgc $ \pool -> do
          P.withResource pool $ checkTables logg True

  where
    opts = info ((richListP <|> migrateOnlyP <|> checkSchemaP <|> envP) <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
    config level = defaultLoggerConfig
      & loggerConfigThreshold .~ level
    backendConfig = defaultHandleBackendConfig
    getLevel = \case
      Args _ _ _ _ level _ _ -> level
      RichListArgs _ level _ -> level
      MigrateOnly _ level _ -> level
      CheckSchema _ level -> level

migrationFiles :: [(FilePath, BS.ByteString)]
migrationFiles = $(embedDir "db-schema/migrations")

initSql :: BS.ByteString
initSql = $(embedFile "db-schema/init.sql")

runMigrations ::
  P.Pool Connection ->
  LogFunctionIO Text ->
  Mg.MigrationAction ->
  Maybe MigrationsFolder ->
  IO ()
runMigrations pool logg migAction mbMigFolder = do

  P.withResource pool $ \conn -> do
    query_ conn "SELECT to_regclass('blocks') :: text" >>= \case
      [Only (Just ("blocks" :: Text))] -> do
        logg Debug "blocks table exists, checking if migration initialization has been performed"
        query_ conn "SELECT to_regclass('schema_migrations') :: text" >>= \case
          [Only (Just ("schema_migrations" :: Text))] -> do
            logg Debug "schema_migrations table exists"
          _ -> do
            logg Error "schema_migrations table does not exist"
            logg Error "The chainweb-data database seems to be created by a chainweb-data version \
                       \prior to the transition to the script-based migration system. Please run \
                       \the migration transition release (2.1.0) to migrate your database to the \
                       \transition state. See https://github.com/kadena-io/chainweb-data/releases/tag/v2.1.0"
            exitFailure
      _ -> do
        logg Info "blocks table does not exist, running init.sql to initialize an empty database"
        void $ execute_ conn (PG.Query initSql)

  files <- case mbMigFolder of
    Just migFolder -> getDir migFolder
    Nothing -> return migrationFiles

  let steps = map (uncurry Mg.MigrationStep) files

  Mg.runMigrations migAction steps pool logg

  P.withResource pool $ checkTables logg False

  logg Info "DB Tables Initialized"

{-
λ> :main single --chain 2 --height 1487570 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main single --chain 0 --height 1494311 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main server --port 9999 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
-}
