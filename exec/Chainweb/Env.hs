{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Chainweb.Env
  ( Env(..)
  , DBPath(..)
  , Url(..)
  , Command(..)
  , envP
  ) where

import Data.String (IsString)
import Options.Applicative

---

data Env = Env Command DBPath Url

newtype DBPath = DBPath String deriving newtype (IsString)

newtype Url = Url String deriving newtype (IsString)

data Command = Server | Update

envP :: Parser Env
envP = Env
  <$> commands
  <*> strOption (long "database" <> metavar "PATH" <> help "Path to database file")
  <*> strOption (long "url" <> metavar "URL" <> help "Url of Chainweb node")

commands :: Parser Command
commands = subparser
  (  command "server" (info (pure Server) (progDesc "Start the analysis server"))
  <> command "update" (info (pure Update) (progDesc "Process all queued Header data"))
  )
