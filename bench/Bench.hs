{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Main where

import           Control.Concurrent
import           Control.Exception
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL (ByteString)
import           Data.Pool
import           Data.String
import           Data.String.Conv
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Time.Clock
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.Beam
import qualified Database.Beam.AutoMigrate as BA
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Syntax
import           Options.Applicative
import           Text.Printf
import           System.Exit
--------------------------------------------------------------------------------
import           ChainwebData.Pagination
import           ChainwebDb.Database
import           ChainwebDb.Queries

main :: IO ()
main = do
    execParser opts >>= \(Args pgc ms) -> do
      infoPrint "Running query benchmarks"
      withPool pgc $ \pool -> do
        withResource pool (bench_initializeTables ms (infoPrint . T.unpack) (errorPrint . T.unpack)) >>= \case
          False -> do
            errorPrint "Cannot run benchmarks on mangled database schemas"
            exitFailure
          True -> do
            infoPrint "Table check done"
            infoPrint "Running benchmarks for code search queries"
            searchTxsBench pool >>= V.mapM_ print
            infoPrint "Running benchmarks for event search queries"
            eventsBench pool >>= V.mapM_ print
  where
    opts = info (argsP <**> helper)
      (fullDesc <> header "chainweb-data benchmarks")

data Args = Args
  {
    args_connectInfo :: ConnectInfo
  , args_migrate :: Bool
  }

infoPrint :: String -> IO ()
infoPrint = printf "[INFO] %s\n"

debugPrint :: String -> IO ()
debugPrint = printf "[DEBUG] %s\n"

errorPrint :: String -> IO ()
errorPrint = printf "[ERROR] %s\n"

argsP :: Parser Args
argsP = Args <$> connectInfoParser <*> migrationP

migrationP :: Parser Bool
migrationP =
  flag True False (short 'm' <> long "migrate" <> help "Run DB migration")

connectInfoParser :: Parser ConnectInfo
connectInfoParser = ConnectInfo
  <$> strOption (long "host" <> metavar "HOST" <> help "host for the chainweb-data postgres instance")
  <*> option auto (long "port" <> metavar "PORT" <> help "port for the chainweb-data postgres instance")
  <*> strOption (long "user" <> metavar "USER" <> value "postgres" <> help "user for the chainweb-data postgres instance")
  <*> strOption (long "password" <> metavar "PASSWORD" <> value "" <> help "password for the chainweb-data postgres instance")
  <*> strOption (long "database" <> metavar "DATABASE" <> help "database for the chainweb-data postgres instance")

-- | A bracket for `Pool` interaction.
withPool :: ConnectInfo -> (Pool Connection -> IO a) -> IO a
withPool ci = bracket (getPool (connect ci)) destroyAllResources

-- | Create a `Pool` based on `Connect` settings designated on the command line.
getPool :: IO Connection -> IO (Pool Connection)
getPool getConn = do
  caps <- getNumCapabilities
  createPool getConn close 1 5 caps

searchTxsBench :: Pool Connection -> IO (Vector BenchResult)
searchTxsBench pool =
    withResource pool $ \conn -> do
      V.forM benchParams $ \(l,o,s) -> do
        let stmt' = prependExplainAnalyze (stmt l o s)
        res <- query_ @(Only ByteString) conn stmt'
        return $ BenchResult stmt' res
  where
    stmt l o s = Query $ toS $ selectStmtString $ searchTxsQueryStmt l o s
    prependExplainAnalyze = ("EXPLAIN (ANALYZE) " <>)
    benchParams =
      V.fromList [ (l,o,s) | l <- (Just . Limit) <$> [40] , o <- (Just . Offset) <$> [20], s <- take 1 searchExamples ]

eventsBench :: Pool Connection -> IO (Vector BenchResult)
eventsBench pool =
  withResource pool $ \conn ->
    V.forM benchParams $ \(l,o,s) -> do
      let stmt' = prependExplainAnalyze (stmt l o s)
      res <- query_ @(Only ByteString) conn stmt'
      return $ BenchResult stmt' res
  where
    stmt l o s = Query $ toS $ selectStmtString $ eventsQueryStmt l o s Nothing Nothing
    prependExplainAnalyze = ("EXPLAIN (ANALYZE) " <>)
    benchParams =
      V.fromList [ (l,o,s) | l <- (Just . Limit) <$> [40] , o <- (Just . Offset) <$> [20], s <- Just <$> drop 2 searchExamples ]

selectStmtString s = case s of
  SqlSelect ss -> pgRenderSyntaxScript $ fromPgSelect $ ss

data BenchResult = BenchResult
  {
    bench_query :: Query
  , bench_explain_analyze_report :: [Only ByteString]
  } deriving Show

searchExamples :: [Text]
searchExamples = ["module", "hat", "coin"]
