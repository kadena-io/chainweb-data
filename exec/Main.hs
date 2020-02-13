{-# LANGUAGE BangPatterns #-}

module Main where

import BasePrelude
import Chainweb.Backfill (backfill)
import Chainweb.Database (initializeTables)
import Chainweb.Env
import Chainweb.New (ingest)
import Chainweb.Update (updates)
import Control.Exception (bracket)
import Database.Beam.Postgres (close, connect)
import Network.HTTP.Client
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Options.Applicative

---

-- Note: The `bracket` here catches exceptions (including Ctrl+C) and safely
-- closes the connection to the database.
main :: IO ()
main = do
  Args c ci u v <- execParser opts
  bracket (connect ci) close $ \conn -> do
    initializeTables conn
    putStrLn "DB Tables Initialized"
    m <- newManager tlsManagerSettings
    let !env = Env m conn u v
    case c of
      Listen -> ingest env
      Worker -> updates env
      Backfill -> backfill env
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
