module Chainweb.Single ( single ) where

import BasePrelude
import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.Common (BlockHeight)
import Chainweb.Env
import Chainweb.Lookups
import Chainweb.Worker (writeBlock)

---

-- | Look up a single chain/height pair, and write all blocks that were found
-- there.
single :: Env -> ChainId -> BlockHeight -> IO ()
single e@(Env _ pool _ _ _) cid h = do
  count <- newIORef 0
  headersBetween e (cid, Low h, High h) >>= traverse_ (writeBlock e pool count)
  final <- readIORef count
  printf "[INFO] Filled in %d blocks.\n" final
