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
import           Chainweb.Gaps (gaps)
import           Chainweb.Listen (listen)
import           Chainweb.Server (apiServer)
import           Chainweb.Lookups (getNodeInfo)
import           Chainweb.RichList (richList)
import           Chainweb.Single (single)
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
    withLogger config backend $ \logger -> do
      let logg = loggerFunIO logger
      case args of
        RichListArgs (NodeDbPath mfp) -> do
          fp <- case mfp of
            Nothing -> do
              h <- getHomeDirectory
              let h' = h </> ".local/share"
              logg Info $ "[INFO] Constructing rich list using default db-path: " <> fromString h'
              -- putStrLn $ "[INFO] Constructing rich list using default db-path: " <> h'
              return h'
            Just fp -> do
              logg Info $ "[INFO] Constructing rich list using given db-path: " <> fromString fp
              return fp
          richList logg fp
        Args c pgc us u -> do
          logg Info $ "Using database: " <> fromString (show pgc)
          logg Info $ "Service API: " <> fromString (showUrlScheme us)
          logg Info $ "P2P API: " <> fromString (showUrlScheme (UrlScheme Https u))
          withPool pgc $ \pool -> do
            P.withResource pool initializeTables
            logg Info "DB Tables Initialized"
            let mgrSettings = mkManagerSettings (TLSSettingsSimple True False False) Nothing
            m <- newManager mgrSettings
            getNodeInfo m us >>= \case
              Left e -> logg Error (fromString $ printf "[FAIL] Unable to connect to %s /info endpoint\n%s" (showUrlScheme us) e) >> exitFailure
              Right ni -> do
                let !mcids = map (second (map (ChainId . fst))) <$> _nodeInfo_graphs ni
                case mcids of
                  Nothing -> logg Error "[FAIL] Node did not have graph information" >> exitFailure
                  Just cids -> do
                    let !env = Env m pool us u ni cids logg
                    case c of
                      Listen -> listen env
                      Backfill as -> backfill env as
                      Gaps rateLimit -> gaps env rateLimit
                      Single cid h -> single env cid h
                      Server serverEnv -> apiServer env serverEnv
  where
    opts = info ((richListP <|> envP) <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
    config = defaultLoggerConfig
    backendConfig = defaultHandleBackendConfig


{-
λ> :main single --chain 2 --height 1487570 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main single --chain 0 --height 1494311 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
λ> :main server --port 9999 --service-host api.chainweb.com --p2p-host us-e3.chainweb.com --dbname chainweb-data --service-port 443 --service-https
-}
