{-# LANGUAGE LambdaCase #-}
module Chainweb.Single ( single ) where

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.Common (BlockHeight)
import Chainweb.Env
import Chainweb.Lookups
import Chainweb.Worker (writeBlock)
import ChainwebData.Types
import Data.IORef
import Data.Foldable
import Text.Printf

---

-- | Look up a single chain/height pair, and write all blocks that were found
-- there.
single :: Env -> ChainId -> BlockHeight -> IO ()
single env cid h = do
  count <- newIORef 0
  let pool = _env_dbConnPool env
      range = (cid, Low h, High h)
  headersBetween env range >>= \case
        Left e -> printf "[FAIL] ApiError for range %s: %s\n" (show range) (show e)
        Right [] -> printf "[FAIL] headersBetween: %s\n" $ show range
        Right hs -> traverse_ (writeBlock env pool count) hs
  final <- readIORef count
  printf "[INFO] Filled in %d blocks.\n" final
