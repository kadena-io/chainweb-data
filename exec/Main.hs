module Main ( main ) where

import Chainweb.Env (Command(..), Env(..), envP)
import Chainweb.Server (server)
import Chainweb.Update (update)
import Options.Applicative

---

main :: IO ()
main = do
  Env c d u <- execParser opts
  case c of
    Server -> server d u
    Update -> update
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
