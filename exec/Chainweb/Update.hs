{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Update ( update ) where

import Chainweb.Api.BlockPayload
import Chainweb.Api.ChainId
import Chainweb.Api.MinerData
import Chainweb.Api.Transaction
import ChainwebDb.Types.Block
import ChainwebDb.Types.Header
import Data.Text (Text)
import Database.SQLite.Simple (Connection)
import Network.HTTP.Client (Manager)
import Servant.API hiding (Header)
import Servant.Client

---

update :: Manager -> Connection -> IO ()
update m c = do
  putStrLn "Update goes here."

blockify :: Header -> IO Block
blockify h = do
  undefined

payload :: Manager -> Header -> IO BlockPayload
payload m h = do
  undefined

--------------------------------------------------------------------------------
-- Endpoints

type PayloadAPI = "chainweb"
  :> "0.0"
  :> Capture "version" Text
  :> "chain"
  :> Capture "chainId" ChainId
  -- TODO More here
  :> Get '[JSON] BlockPayload
