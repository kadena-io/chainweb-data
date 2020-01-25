module Chainweb.Update where

import Database.SQLite.Simple (Connection)

---

update :: Connection -> IO ()
update c = do
  putStrLn "Update goes here."
