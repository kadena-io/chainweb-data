{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Main where

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
import           GHC.Conc (getNumProcessors)
import           Options.Applicative
import           Text.Printf
import           System.Exit
--------------------------------------------------------------------------------
import           ChainwebData.Api
import           ChainwebData.Pagination
import           ChainwebDb.Database
import           ChainwebDb.Queries

main :: IO ()
main = do
    execParser opts >>= \(Args pgc ms rb pr) -> do
      infoPrint "Running query benchmarks"
      withPool pgc $ \pool -> do
        withResource pool (bench_initializeTables ms (infoPrint . T.unpack) (errorPrint . T.unpack)) >>= \case
          False -> do
            errorPrint "Cannot run benchmarks on mangled database schemas"
            exitFailure
          True -> do
            infoPrint "Table check done"
            case rb of
              OnlyEvent es -> do
                infoPrint "Running benchmarks for event search queries"
                eventsBench pool es >>= V.mapM_ (`printReport` pr)
              OnlyCode cs -> do
                infoPrint "Running benchmarks for code search queries"
                searchTxsBench pool cs >>= V.mapM_ (`printReport` pr)
              Both es cs -> do
                infoPrint "Running benchmarks for code search queries"
                searchTxsBench pool cs >>= V.mapM_ (`printReport` pr)
                infoPrint "Running benchmarks for event search queries"
                eventsBench pool es >>= V.mapM_ (`printReport` pr)
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
  , args_run_benches :: RunBenches
  , args_print_report :: ReportFormat
  }


data RunBenches = OnlyEvent [Text] | OnlyCode [Text] | Both [Text] [Text]

data ReportFormat = Simple | Raw

infoPrint :: String -> IO ()
infoPrint = printf "[INFO] %s\n"

debugPrint :: String -> IO ()
debugPrint = printf "[DEBUG] %s\n"

errorPrint :: String -> IO ()
errorPrint = printf "[ERROR] %s\n"

argsP :: Parser Args
argsP = Args <$> connectInfoParser <*> migrationP <*> runBenchesP <*> printReportP

runBenchesP :: Parser RunBenches
runBenchesP = go <$> many eventBenchP <*> many codeBenchP
  where
    go xs [] = OnlyEvent xs
    go [] ys = OnlyCode ys
    go xs ys = Both xs ys

eventBenchP :: Parser Text
eventBenchP = strOption (short 'e' <> long "event-search-query" <> metavar "STRING" <> help "event search query")

codeBenchP :: Parser Text
codeBenchP = strOption (short 'c' <> long "code-search-query" <> metavar "STRING" <> help "code search query")

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
  caps <- getNumProcessors
  createPool getConn close 1 5 caps

searchTxsBench :: Pool Connection -> [Text] -> IO (Vector BenchResult)
searchTxsBench pool qs =
    withResource pool $ \conn -> do
      V.forM benchParams $ \(l,o,s) -> do
        let stmt' = prependExplainAnalyze (stmt l o s)
        res <- query_ @(Only ByteString) conn stmt'
        return $ getBenchResult (Just s) "Code search" stmt' res
  where
    stmt l o s = Query $ toS $ selectStmtString $ searchTxsQueryStmt l o s
    prependExplainAnalyze = ("EXPLAIN (ANALYZE) " <>)
    benchParams =
      V.fromList [ (l,o,s) | l <- (Just . Limit) <$> [40] , o <- [Nothing], s <- qs `onNull` (take 2 searchExamples) ]

onNull :: [a] -> [a] -> [a]
onNull xs ys = case xs of
  [] -> ys
  _ -> xs

eventsBench :: Pool Connection -> [Text] -> IO (Vector BenchResult)
eventsBench pool qs =
  withResource pool $ \conn ->
    V.forM benchParams $ \(l,o,s) -> do
      let stmt' = prependExplainAnalyze (stmt l o s)
      res <- query_ @(Only ByteString) conn stmt'
      return $ getBenchResult s "Event search" stmt' res
  where
    stmt l o s = Query $ toS $ selectStmtString $ eventsQueryStmt l o s Nothing (Just $ EventName "coin.TRANSFER")
    prependExplainAnalyze = ("EXPLAIN (ANALYZE) " <>)
    benchParams =
      V.fromList [ (l,o,s) | l <- (Just . Limit) <$> [40] , o <- [Nothing], s <- Just <$> qs `onNull` drop 2 searchExamples ]


getBenchResult :: Maybe Text -> ByteString -> Query -> [Only ByteString] -> BenchResult
getBenchResult simple_param name q = go . fmap fromOnly . reverse
  where
    go (pl: ex: report) =
      BenchResult
        {
          bench_query = name
        , bench_raw_query = q
        , bench_simplified_query = simple_param
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
  , bench_simplified_query :: Maybe Text
  , bench_explain_analyze_report :: ByteString
  , bench_planning_time :: ByteString
  , bench_execution_time :: ByteString
  } deriving Show

searchExamples :: [Text]
searchExamples = [ "receiver-guard"
                 , "transfer-crosschain"
                 , "module"
                 , "hat"
                 , "99cb7008d7d70c94f138cc366a825f0d9c83a8a2f4ba82c86c666e0ab6fecf3a"
                 , "40ab110e52d0221ec8237d16f4b415fa52b8df97b26e6ae5d3518854a4a8d30f"]
