{-# LANGUAGE BangPatterns #-}

module Main where

import BasePrelude
import Chainweb.Backfill (backfill)
import Chainweb.Database (initializeTables)
import Chainweb.Env
import Chainweb.New (ingest)
import Chainweb.Update (updates)
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
    let !env = Env m pgc u v
    case c of
      Listen -> ingest env
      Worker -> updates env
      Backfill -> backfill env
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
