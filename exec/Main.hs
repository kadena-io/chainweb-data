module Main where

import Chainweb.Database (initializeTables)
import Chainweb.Env
import Chainweb.Server (server)
import Chainweb.Update (updates)
import Control.Exception (bracket)
import Database.SQLite.Simple (close, open)
import Network.HTTP.Client
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Options.Applicative

---

main :: IO ()
main = do
  Env c (DBPath d) u v <- execParser opts
  bracket (open d) close $ \conn -> do
    initializeTables conn
    m <- newManager tlsManagerSettings
    case c of
      Server -> server m conn u
      Update -> updates m conn u v
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
