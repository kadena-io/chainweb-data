{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Env
  ( Args(..)
  , Env(..)
  , Connect(..), withConnection, withPool
  , Url(..)
  , ChainwebVersion(..)
  , Command(..)
  , envP
  ) where

import BasePrelude hiding (option)
import Chainweb.Api.ChainId (ChainId(..))
import Chainweb.Api.Common (BlockHeight)
import Data.ByteString (ByteString)
import Data.Pool
import Data.Text (Text)
import Database.Beam.Postgres
import Network.HTTP.Client (Manager)
import Options.Applicative
import Text.Printf

---

data Args = Args Command Connect Url ChainwebVersion

data Env = Env
  { _env_httpManager :: Manager
  , _env_dbConnectInfo :: Connect
  , _env_nodeUrl :: Url
  , _env_chainwebVersion :: ChainwebVersion
  , _env_chains :: NonEmpty ChainId
  }

data Connect = PGInfo ConnectInfo | PGString ByteString

-- | Open a postgres database connection.
getConnection :: Connect -> IO Connection
getConnection (PGInfo ci) = connect ci
getConnection (PGString s) = connectPostgreSQL s

-- | A bracket for `Connection` interaction.
withConnection :: Connect -> (Connection -> IO a) -> IO a
withConnection c = bracket (getConnection c) close

-- | Create a `Pool` based on `Connect` settings designated on the command line.
getPool :: Connect -> IO (Pool Connection)
getPool c = do
  caps <- getNumCapabilities
  createPool (getConnection c) close 1 5 caps

-- | A bracket for `Pool` interaction.
withPool :: Connect -> (Pool Connection -> IO a) -> IO a
withPool c = bracket (getPool c) destroyAllResources

newtype Url = Url String
  deriving newtype (IsString, PrintfArg)

newtype ChainwebVersion = ChainwebVersion Text
  deriving newtype (IsString)

data Command = Server | Listen | Backfill | Gaps | Single ChainId BlockHeight

envP :: Parser Args
envP = Args
  <$> commands
  <*> connectP
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")
  <*> strOption (long "version" <> metavar "VERSION" <> value "mainnet01" <> help "Network Version")

connectP :: Parser Connect
connectP = (PGString <$> pgstringP) <|> (PGInfo <$> connectInfoP)

pgstringP :: Parser ByteString
pgstringP = strOption (long "dbstring" <> help "Postgres Connection String")

-- | These defaults are pulled from the postgres-simple docs.
connectInfoP :: Parser ConnectInfo
connectInfoP = ConnectInfo
  <$> strOption   (long "dbhost" <> value "localhost" <> help "Postgres DB hostname")
  <*> option auto (long "dbport" <> value 5432        <> help "Postgres DB port")
  <*> strOption   (long "dbuser" <> value "postgres"  <> help "Postgres DB user")
  <*> strOption   (long "dbpass" <> value ""          <> help "Postgres DB password")
  <*> strOption   (long "dbname" <> value "postgres"  <> help "Postgres DB name")

singleP :: Parser Command
singleP = Single
  <$> (ChainId <$> option auto (long "chain" <> metavar "INT"))
  <*> option auto (long "height" <> metavar "INT")

commands :: Parser Command
commands = hsubparser
  (  command "listen" (info (pure Listen)
       (progDesc "Node Listener - Waits for new blocks and adds them to work queue"))
  <> command "backfill" (info (pure Backfill)
       (progDesc "Backfill Worker - Backfills blocks from before DB was started"))
  <> command "gaps" (info (pure Gaps)
       (progDesc "Gaps Worker - Fills in missing blocks lost during backfill or listen"))
  <> command "single" (info singleP
       (progDesc "Single Worker - Lookup and write the blocks at a given chain/height"))
  <> command "server" (info (pure Server)
       (progDesc "Serve the chainweb-data REST API"))
  )
