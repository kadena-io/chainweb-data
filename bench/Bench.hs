{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Main where

import           Control.Concurrent
import           Control.Exception
import           Data.Char
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL (ByteString)
import           Data.Pool
import           Data.String.Conv
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.Beam.Backend.SQL
import           Database.Beam
-- import qualified Database.Beam.AutoMigrate as BA
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
    execParser opts >>= \(Args pgc ms pr) -> do
      infoPrint "Running query benchmarks"
      withPool pgc $ \pool -> do
        withResource pool (bench_initializeTables ms (infoPrint . T.unpack) (errorPrint . T.unpack)) >>= \case
          False -> do
            errorPrint "Cannot run benchmarks on mangled database schemas"
            exitFailure
          True -> do
            infoPrint "Table check done"
            infoPrint "Running benchmarks for code search queries"
            searchTxsBench pool >>= V.mapM_ (`printReport` pr)
            infoPrint "Running benchmarks for event search queries"
            eventsBench pool >>= V.mapM_ (`printReport` pr)
  where
    opts = info (argsP <**> helper)
      (fullDesc <> header "chainweb-data benchmarks")

printReport :: BenchResult -> ReportFormat -> IO ()
printReport br@(BenchResult {..}) = \case
    Raw -> print br
    Simple -> do
      BC.putStrLn "----------RESULT----------"
      BC.putStrLn $ "Query: " <> bench_query
      BC.putStrLn bench_execution_time
      BC.putStrLn bench_planning_time
      BC.putStrLn "----------RESULT----------"

data Args = Args
  {
    args_connectInfo :: ConnectInfo
  , args_migrate :: Bool
  , args_print_report :: ReportFormat
  }

data ReportFormat = Simple | Raw

infoPrint :: String -> IO ()
infoPrint = printf "[INFO] %s\n"

debugPrint :: String -> IO ()
debugPrint = printf "[DEBUG] %s\n"

errorPrint :: String -> IO ()
errorPrint = printf "[ERROR] %s\n"

argsP :: Parser Args
argsP = Args <$> connectInfoParser <*> migrationP <*> printReportP

printReportP :: Parser ReportFormat
printReportP = option go (short 'q' <> long "query-report-format (raw|simple)" <> value Simple <> help "print query report")
  where
    go = eitherReader $ \s -> case toLower <$> s of
      "raw" -> Right Raw
      "simple" -> Right Simple
      _ -> Left $ printf "not a valid option (%s)" s

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
        return $ getBenchResult "Code search" stmt' res
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
      return $ getBenchResult "Event search" stmt' res
  where
    stmt l o s = Query $ toS $ selectStmtString $ eventsQueryStmt l o s Nothing Nothing
    prependExplainAnalyze = ("EXPLAIN (ANALYZE) " <>)
    benchParams =
      V.fromList [ (l,o,s) | l <- (Just . Limit) <$> [40] , o <- (Just . Offset) <$> [20], s <- Just <$> drop 2 searchExamples ]


getBenchResult :: ByteString -> Query -> [Only ByteString] -> BenchResult
getBenchResult name q = go . fmap fromOnly . reverse
  where
    go (pl: ex: report) =
      BenchResult
        {
          bench_query = name
        , bench_raw_query = q
        , bench_explain_analyze_report = BC.unlines $ reverse report
        , bench_planning_time = pl
        , bench_execution_time = ex
        }
    go _ = error "getBenchResult: impossible"

selectStmtString :: (Sql92SelectSyntax (BeamSqlBackendSyntax be) ~ PgSelectSyntax) => SqlSelect be a -> BL.ByteString
selectStmtString s = case s of
  SqlSelect ss -> pgRenderSyntaxScript $ fromPgSelect $ ss

data BenchResult = BenchResult
  {
    bench_query :: ByteString
  , bench_raw_query :: Query
  , bench_explain_analyze_report :: ByteString
  , bench_planning_time :: ByteString
  , bench_execution_time :: ByteString
  } deriving Show

searchExamples :: [Text]
searchExamples = ["module"
                 , "hat"
                 , "99cb7008d7d70c94f138cc366a825f0d9c83a8a2f4ba82c86c666e0ab6fecf3a"
                 , "40ab110e52d0221ec8237d16f4b415fa52b8df97b26e6ae5d3518854a4a8d30f"]
