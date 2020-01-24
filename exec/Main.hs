module Main ( main ) where

import Chainweb.Env
import Chainweb.Server (server)
import Options.Applicative

---

main :: IO ()
main = do
  Env c d u <- execParser opts
  case c of
    Server -> server d u
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")
