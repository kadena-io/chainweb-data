module Chainweb.Single ( single ) where

import BasePrelude
import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.Common (BlockHeight)
import Chainweb.Env
import Chainweb.Lookups
import Chainweb.Worker (writeBlock)
import ChainwebData.Types
import System.Logger.Types hiding (logg)

---

-- | Look up a single chain/height pair, and write all blocks that were found
-- there.
single :: Env -> ChainId -> BlockHeight -> IO ()
single e cid h = do
    count <- newIORef 0
    let pool = _env_dbConnPool e
    headersBetween e (cid, Low h, High h) >>= traverse_ (writeBlock e pool count)
    final <- readIORef count
    logg Info $ fromString $ printf "[INFO] Filled in %d blocks.\n" final
  where
    logg = _env_logger e
