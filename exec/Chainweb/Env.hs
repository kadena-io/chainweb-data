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

data Command = New | Update | Backfill

envP :: Parser Args
envP = Args
  <$> commands
  <*> strOption (long "database" <> metavar "PATH" <> help "Path to database file")
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")
  <*> strOption (long "version" <> metavar "VERSION" <> value "mainnet01" <> help "Network Version")

commands :: Parser Command
commands = subparser
  (  command "new"    (info (pure New)      (progDesc "Start the analysis server"))
  <> command "update" (info (pure Update)   (progDesc "Process all queued Header data"))
  <> command "old"    (info (pure Backfill) (progDesc "Backfill all missing Blocks"))
  )
