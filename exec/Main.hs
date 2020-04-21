{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

module Main where

import           BasePrelude
import           Chainweb.Backfill (backfill)
import           Chainweb.Database (initializeTables)
import           Chainweb.Env
import           Chainweb.Gaps (gaps)
import           Chainweb.Listen (listen)
import           Chainweb.Server (apiServer)
import           Chainweb.Lookups (allChains)
import           Chainweb.Single (single)
import qualified Data.Pool as P
import           Network.Connection
import           Network.HTTP.Client hiding (withConnection)
import           Network.HTTP.Client.TLS
import           Options.Applicative

---

main :: IO ()
main = do
  Args c pgc u v <- execParser opts
  putStrLn $ "Using database: " <> show pgc
  withPool pgc $ \pool -> do
    P.withResource pool initializeTables
    putStrLn "DB Tables Initialized"
    let mgrSettings = mkManagerSettings (TLSSettingsSimple True False False) Nothing
    m <- newManager mgrSettings
    allChains m u >>= \case
      Nothing -> printf "[FAIL] Unable to connect to %s\n" (urlToString u) >> exitFailure
      Just cids -> do
        let !env = Env m pool u v cids
        case c of
          Listen -> listen env
          Backfill -> backfill env
          Gaps -> gaps env
          Single cid h -> single env cid h
          Server serverEnv -> apiServer env serverEnv
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
