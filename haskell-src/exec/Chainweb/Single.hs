{-# LANGUAGE LambdaCase #-}
module Chainweb.Single ( single ) where

import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.Common (BlockHeight)
import ChainwebData.Env
import Chainweb.Lookups
import Chainweb.Worker (writeBlock)
import ChainwebData.Types
import System.Logger.Types hiding (logg)
import Data.IORef
import Data.Foldable
import Data.String
import Text.Printf

---

-- | Look up a single chain/height pair, and write all blocks that were found
-- there.
single :: Env -> ChainId -> BlockHeight -> IO ()
single env cid h = do
  count <- newIORef 0
  let pool = _env_dbConnPool env
      range = (cid, Low h, High h)
      logg = _env_logger env
  headersBetween env range >>= \case
        Left e -> logg Error $ fromString $ printf "ApiError for range %s: %s" (show range) (show e)
        Right [] -> logg Error $ fromString $ printf "headersBetween: %s" $ show range
        Right hs -> traverse_ (writeBlock env pool count) hs
  final <- readIORef count
  logg Info $ fromString $ printf "Filled in %d blocks." final
