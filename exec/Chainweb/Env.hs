{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Env
  ( Env(..)
  , DBPath(..)
  , Url(..)
  , ChainwebVersion(..)
  , Command(..)
  , envP
  ) where

import BasePrelude
import Data.Text (Text)
import Options.Applicative

---

data Env = Env Command DBPath Url ChainwebVersion

newtype DBPath = DBPath String
  deriving newtype (IsString)

newtype Url = Url String
  deriving newtype (IsString)

newtype ChainwebVersion = ChainwebVersion Text
  deriving newtype (IsString)

data Command = Server | Update

envP :: Parser Env
envP = Env
  <$> commands
  <*> strOption (long "database" <> metavar "PATH" <> help "Path to database file")
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")
  <*> strOption (long "version" <> metavar "VERSION" <> value "mainnet01" <> help "Network Version")

commands :: Parser Command
commands = subparser
  (  command "server" (info (pure Server) (progDesc "Start the analysis server"))
  <> command "update" (info (pure Update) (progDesc "Process all queued Header data"))
  )
