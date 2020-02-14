{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Env
  ( Args(..)
  , Env(..)
  , Connect(..)
  , Url(..)
  , ChainwebVersion(..)
  , Command(..)
  , envP
  ) where

import BasePrelude hiding (option)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Database.Beam.Postgres (ConnectInfo(..), Connection)
import Network.HTTP.Client (Manager)
import Options.Applicative

---

data Args = Args Command Connect Url ChainwebVersion

data Env = Env Manager Connection Url ChainwebVersion

data Connect = PGInfo ConnectInfo | PGString ByteString

newtype Url = Url String
  deriving newtype (IsString)

newtype ChainwebVersion = ChainwebVersion Text
  deriving newtype (IsString)

data Command = Listen | Worker | Backfill

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

commands :: Parser Command
commands = subparser
  (  command "listen"   (info (pure Listen)   (progDesc "Node Listener - Waits for new blocks and adds them to work queue"))
  <> command "worker"   (info (pure Worker)   (progDesc "New Block Worker - Removes blocks from queue and adds them to DB"))
  <> command "backfill" (info (pure Backfill) (progDesc "Backfill Worker - Backfills blocks from before DB was started"))
  )
