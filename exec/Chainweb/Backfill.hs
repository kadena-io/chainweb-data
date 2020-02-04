module Chainweb.Backfill where

import BasePrelude
import Chainweb.Env (Env(..))
import ChainwebDb.Types.DbHash

---

newtype Parent = Parent DbHash

backfill :: Env -> IO ()
backfill _ = putStrLn "Not implemented."

{-

1. Get a confirmed block from the database.
2. Is its parent also in the database, or least in the work queue?
3. If yes, move to the next confirmed block.
4. If no, fetch the Header from the Node and write it to the queue.
5. Recurse on (2) with the parent of the Header we just wrote.

-}

work :: Env -> Parent -> IO ()
work = undefined
