module Main where

import Chainweb.Database (initializeTables)
import Chainweb.Env
import Chainweb.Server (server)
import Chainweb.Update (update)
import Control.Exception (bracket)
import Database.SQLite.Simple (close, open)
import Options.Applicative

---

main :: IO ()
main = do
  Env c (DBPath d) u <- execParser opts
  bracket (open d) close $ \conn -> do
    initializeTables conn
    case c of
      Server -> server conn u
      Update -> update conn
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
