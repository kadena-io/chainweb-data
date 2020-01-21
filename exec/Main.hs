{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TypeApplications   #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- TODO Remove if orphan is adopted.

module Main ( main ) where

import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.BytesLE
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           ChainwebDb.Types.Block (BlockT(..))
import           ChainwebDb.Types.DbHash (DbHash(..))
import           Control.Exception (bracket)
import           Data.Aeson (ToJSON(..), object)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Database.Beam
import           Database.Beam.Migrate
import           Database.Beam.Migrate.Simple
import           Database.Beam.Sqlite
    (Sqlite, runBeamSqlite, runBeamSqliteDebug)
import           Database.Beam.Sqlite.Migrate (migrationBackend)
import           Database.SQLite.Simple (Connection, close, open)
import           Lens.Micro ((^?))
import           Lens.Micro.Aeson (key, _JSON)
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (tlsManagerSettings)
import           Network.Wai.EventSource.Streaming
import           Servant.Client.Core (BaseUrl(..), Scheme(..))
import qualified Streaming.Prelude as SP

--------------------------------------------------------------------------------
-- Environment

databaseFile :: String
databaseFile = "chainweb-data.db"

url :: BaseUrl
url = BaseUrl Https "honmono.fosskers.ca" 443 ""

--------------------------------------------------------------------------------
-- Work

main :: IO ()
main = do
  m <- newManager tlsManagerSettings
  bracket (open databaseFile) close $ \conn -> do
    initializeTables conn
    ingest m url conn

ingest :: Manager -> BaseUrl -> Connection -> IO ()
ingest m u c = withEvents (req u) m $ SP.mapM_ f . dataOnly @BlockHeader
  where
    f :: BlockHeader -> IO ()
    f bh = runBeamSqliteDebug putStrLn c  -- TODO Take out the debug
      . runInsert
      . insert (blocks $ unCheckDatabase database)
      $ insertExpressions [g bh]

    g :: BlockHeader -> BlockT (QExpr Sqlite s)
    g bh = Block
      { _block_id           = default_
      , _block_creationTime = val_ . floor $ _blockHeader_creationTime bh
      , _block_chainId      = val_ . unChainId $ _blockHeader_chainId bh
      , _block_height       = val_ $ _blockHeader_height bh
      , _block_hash         = val_ . DbHash . hashB64U $ _blockHeader_hash bh
      , _block_target       = val_ . DbHash . hexBytesLE $ _blockHeader_target bh
      , _block_weight       = val_ . DbHash . hexBytesLE $ _blockHeader_weight bh
      , _block_epochStart   = val_ . floor $ _blockHeader_epochStart bh
      , _block_nonce        = val_ $ _blockHeader_nonce bh }

req :: BaseUrl -> Request
req u = defaultRequest
  { host = T.encodeUtf8 . T.pack . baseUrlHost $ u
  , path = "chainweb/0.0/mainnet01/header/updates"  -- TODO Parameterize as needed.
  , port = baseUrlPort u
  , secure = True
  , method = "GET"
  , requestBody = mempty
  , responseTimeout = responseTimeoutNone
  , checkResponse = throwErrorStatusCodes }

-- TODO Adopt this orphan?
instance FromEvent BlockHeader where
  fromEvent bs = bs ^? key "header" . _JSON

instance ToJSON BlockHeader where
  toJSON _ = object []

--------------------------------------------------------------------------------
-- Database Definition

data ChainwebDataDb f = ChainwebDataDb
  { blocks :: f (TableEntity BlockT) }
  deriving stock (Generic)
  deriving anyclass (Database be)

database :: CheckedDatabaseSettings Sqlite ChainwebDataDb
database = defaultMigratableDbSettings

-- | Create the DB tables if necessary.
initializeTables :: Connection -> IO ()
initializeTables conn = runBeamSqlite conn $
  verifySchema migrationBackend database >>= \case
    VerificationFailed _  -> createSchema migrationBackend database
    VerificationSucceeded -> pure ()
