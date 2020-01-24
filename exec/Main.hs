{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- TODO Remove if orphan is adopted.

module Main ( main ) where

import           Chainweb.Api.BlockHeader (BlockHeader(..))
import           Chainweb.Api.BytesLE
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash (DbHash(..))
import           ChainwebDb.Types.Header
import           ChainwebDb.Types.Miner
import           ChainwebDb.Types.Transaction
import           Control.Exception (bracket)
import           Data.Aeson (ToJSON(..), object)
import           Data.String (IsString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Database.Beam
import           Database.Beam.Migrate
import           Database.Beam.Migrate.Simple
import           Database.Beam.Sqlite (Sqlite, runBeamSqlite)
import           Database.Beam.Sqlite.Migrate (migrationBackend)
import           Database.SQLite.Simple (Connection, close, open)
import           Lens.Micro ((^?))
import           Lens.Micro.Aeson (key, _JSON)
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (tlsManagerSettings)
import           Network.Wai.EventSource.Streaming
import           Options.Applicative
import           Servant.Client.Core (BaseUrl(..), Scheme(..))
import qualified Streaming.Prelude as SP
import           Text.Printf (printf)

--------------------------------------------------------------------------------
-- Environment

data Env = Env Command DBPath Url

newtype DBPath = DBPath String deriving newtype (IsString)

newtype Url = Url String deriving newtype (IsString)

data Command = Server

envP :: Parser Env
envP = Env
  <$> commands
  <*> strOption (long "database" <> metavar "PATH" <> help "Path to database file")
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")

commands :: Parser Command
commands = subparser
  (command "server" (info (pure Server) (progDesc "Start the analysis server")))

--------------------------------------------------------------------------------
-- Work

main :: IO ()
main = do
  Env c d u <- execParser opts
  case c of
    Server -> server d u
  where
    opts = info (envP <**> helper)
      (fullDesc <> header "chainweb-data - Processing and analysis of Chainweb data")

server :: DBPath -> Url -> IO ()
server (DBPath d) (Url u) = do
  m <- newManager tlsManagerSettings
  bracket (open d) close $ \conn -> do
    initializeTables conn
    ingest m (BaseUrl Https u 443 "") conn

ingest :: Manager -> BaseUrl -> Connection -> IO ()
ingest m u c = withEvents (req u) m $ SP.mapM_ (\bh -> f bh >> h bh) . dataOnly @BlockHeader
  where
    f :: BlockHeader -> IO ()
    f bh = runBeamSqlite c
      . runInsert
      . insert (headers $ unCheckDatabase dbSettings)
      $ insertExpressions [g bh]

    g :: BlockHeader -> HeaderT (QExpr Sqlite s)
    g bh = Header
      { _header_id           = default_
      , _header_creationTime = val_ . floor $ _blockHeader_creationTime bh
      , _header_chainId      = val_ . unChainId $ _blockHeader_chainId bh
      , _header_height       = val_ $ _blockHeader_height bh
      , _header_hash         = val_ . DbHash . hashB64U $ _blockHeader_hash bh
      , _header_target       = val_ . DbHash . hexBytesLE $ _blockHeader_target bh
      , _header_weight       = val_ . DbHash . hexBytesLE $ _blockHeader_weight bh
      , _header_epochStart   = val_ . floor $ _blockHeader_epochStart bh
      , _header_nonce        = val_ $ _blockHeader_nonce bh }

    h :: BlockHeader -> IO ()
    h bh = printf "Chain %d: %d\n" (unChainId $ _blockHeader_chainId bh) (_blockHeader_height bh)

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

--------------------------------------------------------------------------------
-- Database Definition

data ChainwebDataDb f = ChainwebDataDb
  { headers :: f (TableEntity HeaderT)
  , blocks :: f (TableEntity BlockT)
  , transactions :: f (TableEntity TransactionT)
  , miners :: f (TableEntity MinerT) }
  deriving stock (Generic)
  deriving anyclass (Database be)

dbSettings :: CheckedDatabaseSettings Sqlite ChainwebDataDb
dbSettings = defaultMigratableDbSettings

-- | Create the DB tables if necessary.
initializeTables :: Connection -> IO ()
initializeTables conn = runBeamSqlite conn $
  verifySchema migrationBackend dbSettings >>= \case
    VerificationFailed _ -> createSchema migrationBackend dbSettings
    VerificationSucceeded -> pure ()

--------------------------------------------------------------------------------
-- Orphans

instance FromEvent BlockHeader where
  fromEvent bs = bs ^? key "header" . _JSON

instance ToJSON BlockHeader where
  toJSON _ = object []
