{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Env
  ( Args(..)
  , Env(..)
  , DBPath(..)
  , Url(..)
  , ChainwebVersion(..)
  , Command(..)
  , envP
  ) where

import BasePrelude
import Data.Text (Text)
import Database.SQLite.Simple (Connection)
import Network.HTTP.Client (Manager)
import Options.Applicative

---

data Args = Args Command DBPath Url ChainwebVersion

data Env = Env Manager Connection Url ChainwebVersion

newtype DBPath = DBPath String
  deriving newtype (IsString)

newtype Url = Url String
  deriving newtype (IsString)

newtype ChainwebVersion = ChainwebVersion Text
  deriving newtype (IsString)

data Command = Listen | Worker | Backfill

envP :: Parser Args
envP = Args
  <$> commands
  <*> strOption (long "database" <> metavar "PATH" <> help "Path to database file")
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")
  <*> strOption (long "version" <> metavar "VERSION" <> value "mainnet01" <> help "Network Version")

commands :: Parser Command
commands = subparser
  (  command "listen"   (info (pure Listen)   (progDesc "Node Listener - Waits for new blocks and adds them to work queue"))
  <> command "worker"   (info (pure Worker)   (progDesc "New Block Worker - Removes blocks from queue and adds them to DB"))
  <> command "backfill" (info (pure Backfill) (progDesc "Backfill Worker - Backfills blocks from before DB was started"))
  )
