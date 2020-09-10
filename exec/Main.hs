{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

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
import           Network.Connection
import           Network.HTTP.Client hiding (withConnection)
import           Network.HTTP.Client.TLS
import           Options.Applicative
import           System.Directory
import           System.Exit
import           System.FilePath
import           Text.Printf

---

main :: IO ()
main = do
  args <- execParser opts
  case args of
    RichListArgs fp -> do
      fp' <- case fp == "" of
        True -> do
          h <- getHomeDirectory
          let h' = h </> ".local/share"
          putStrLn $ "Constructing rich list using default db-path: " <> h'
          return h'
        False -> do
          putStrLn $ "Constructing rich list using given db-path: " <> fp
          return fp
      richList fp'
    Args c pgc u -> do
      putStrLn $ "Using database: " <> show pgc
      withPool pgc $ \pool -> do
        P.withResource pool initializeTables
        putStrLn "DB Tables Initialized"
        let mgrSettings = mkManagerSettings (TLSSettingsSimple True False False) Nothing
        m <- newManager mgrSettings
        getNodeInfo m u >>= \case
          Left e -> printf "[FAIL] Unable to connect to %s /info endpoint\n%s" (urlToString u) e >> exitFailure
          Right ni -> do
            let !mcids = map (second (map (ChainId . fst))) <$> _nodeInfo_graphs ni
            case mcids of
              Nothing -> printf "[FAIL] Node did not have graph information" >> exitFailure
              Just cids -> do
                let !env = Env m pool u ni cids
                case c of
                  Listen -> listen env
                  Backfill -> backfill env
                  Gaps -> gaps env
                  Single cid h -> single env cid h
                  Server serverEnv -> apiServer env serverEnv
  where
    opts = info ((richListP <|> envP) <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
