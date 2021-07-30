{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.NodeInfo
import           Chainweb.Backfill (backfill)
import           Chainweb.Database (initializeTables)
import           Chainweb.Env
import           Chainweb.FillEvents (fillEvents)
import           Chainweb.Gaps (gaps)
import           Chainweb.Listen (listen)
import           Chainweb.Lookups (getNodeInfo)
import           Chainweb.RichList (richList)
import           Chainweb.Server (apiServer)
import           Chainweb.Single (single)
import           Control.Lens
import           Data.Bifunctor
import qualified Data.Pool as P
import           Data.String
import           Network.Connection
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
        RichListArgs (NodeDbPath mfp) _ -> do
          fp <- case mfp of
            Nothing -> do
              h <- getHomeDirectory
              let h' = h </> ".local/share"
              logg Info $ "Constructing rich list using default db-path: " <> fromString h'
              return h'
            Just fp -> do
              logg Info $ "Constructing rich list using given db-path: " <> fromString fp
              return fp
          richList logg fp
        Args c pgc us u _ ms -> do
          logg Info $ "Using database: " <> fromString (show pgc)
          logg Info $ "Service API: " <> fromString (showUrlScheme us)
          logg Info $ "P2P API: " <> fromString (showUrlScheme (UrlScheme Https u))
          withPool pgc $ \pool -> do
            P.withResource pool (initializeTables logg ms)
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
                      Gaps rateLimit -> gaps env rateLimit
                      Single cid h -> single env cid h
                      FillEvents as et -> fillEvents env as et
                      Server serverEnv -> apiServer env serverEnv
  where
    opts = info ((richListP <|> envP) <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
    config level = defaultLoggerConfig
      & loggerConfigThreshold .~ level
    backendConfig = defaultHandleBackendConfig
    getLevel = \case
      Args _ _ _ _ level _ -> level
      RichListArgs _ level -> level


{-
λ> :main single --chain 2 --height 1487570 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main single --chain 0 --height 1494311 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main server --port 9999 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
-}
