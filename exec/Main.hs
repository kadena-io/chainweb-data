{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

module Main where

import BasePrelude
import Chainweb.Backfill (backfill)
import Chainweb.Database (initializeTables)
import Chainweb.Env
import Chainweb.Gaps (gaps)
import Chainweb.Listen (listen)
import Chainweb.Lookups (allChains)
import Chainweb.Single (single)
import Network.HTTP.Client hiding (withConnection)
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Options.Applicative

---

main :: IO ()
main = do
  Args c pgc u v <- execParser opts
  withConnection pgc $ \conn -> do
    initializeTables conn
    putStrLn "DB Tables Initialized"
    m <- newManager tlsManagerSettings
    allChains m u >>= \case
      Nothing -> printf "[FAIL] Unable to contact %s for Chain counts\n" u >> exitFailure
      Just cids -> do
        let !env = Env m pgc u v cids
        case c of
          Listen -> listen env
          Backfill -> backfill env
          Gaps -> gaps env
          Single cid h -> single env cid h
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
