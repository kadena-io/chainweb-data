{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
module ChainwebData.Env
  ( MigrateStatus(..)
  , Args(..)
  , Env(..)
  , chainStartHeights
  , ServerEnv(..)
  , Connect(..), withPool
  , Scheme(..)
  , toServantScheme
  , Url(..)
  , urlToString
  , UrlScheme(..)
  , showUrlScheme
  , ChainwebVersion(..)
  , Command(..)
  , BackfillArgs(..)
  , FillArgs(..)
  , envP
  , richListP
  , NodeDbPath(..)
  , progress
  , EventType(..)
  ) where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Api.NodeInfo
import           Control.Concurrent
import           Control.Exception
import           Data.ByteString (ByteString)
import           Data.Char (toLower)
import           Data.IORef
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe
import           Data.String
import           Data.Pool
import           Data.Text (pack, Text)
import           Data.Time.Clock.POSIX
import           Database.Beam.Postgres
import           Gargoyle
import           Gargoyle.PostgreSQL
-- To get gargoyle to give you postgres automatically without having to install
-- it externally, uncomment the below line and comment the above line. Then do
-- the same thing down in withGargoyleDb and uncomment the gargoyle-postgres-nix
-- package in the cabal file.
--import Gargoyle.PostgreSQL.Nix
import           Network.HTTP.Client (Manager)
import           Options.Applicative
import qualified Servant.Client as S
import           System.IO
import           System.Logger.Types hiding (logg)
import           Text.Printf

---

data MigrateStatus = RunMigration | DontMigrate
  deriving (Eq,Ord,Show,Read)

data Args
  = Args Command Connect UrlScheme Url LogLevel MigrateStatus
    -- ^ arguments for all but the richlist command
  | RichListArgs NodeDbPath LogLevel ChainwebVersion
    -- ^ arguments for the Richlist command
  deriving (Show)

data Env = Env
  { _env_httpManager :: Manager
  , _env_dbConnPool :: Pool Connection
  , _env_serviceUrlScheme :: UrlScheme
  , _env_p2pUrl :: Url
  , _env_nodeInfo :: NodeInfo
  , _env_chainsAtHeight :: [(BlockHeight, [ChainId])]
  , _env_logger :: LogFunctionIO Text
  }

chainStartHeights :: [(BlockHeight, [ChainId])] -> Map ChainId BlockHeight
chainStartHeights chainsAtHeight = go mempty chainsAtHeight
  where
    go m [] = m
    go m ((h,cs):rest) = go (foldr (\c -> M.insert c h) m cs) rest

data Connect = PGInfo ConnectInfo | PGString ByteString | PGGargoyle String
  deriving (Eq,Show)

-- | Equivalent to withPool but uses a Postgres DB started by Gargoyle
withGargoyleDb :: FilePath -> (Pool Connection -> IO a) -> IO a
withGargoyleDb dbPath func = do
  --pg <- postgresNix
  let pg = defaultPostgres
  withGargoyle pg dbPath $ \dbUri -> do
    caps <- getNumCapabilities
    let poolConfig =
          PoolConfig
            {
              createResource = connectPostgreSQL dbUri
            , freeResource = close
            , poolCacheTTL = 5
            , poolMaxResources = caps
            }
    pool <- newPool poolConfig
    func pool

-- | Create a `Pool` based on `Connect` settings designated on the command line.
getPool :: IO Connection -> IO (Pool Connection)
getPool getConn = do
  caps <- getNumCapabilities
  let poolConfig =
        PoolConfig
          {
            createResource = getConn
          , freeResource = close
          , poolCacheTTL = 5
          , poolMaxResources = caps
          }
  newPool poolConfig

-- | A bracket for `Pool` interaction.
withPool :: Connect -> (Pool Connection -> IO a) -> IO a
withPool (PGGargoyle dbPath) = withGargoyleDb dbPath
withPool (PGInfo ci) = bracket (getPool (connect ci)) destroyAllResources
withPool (PGString s) = bracket (getPool (connectPostgreSQL s)) destroyAllResources

data Scheme = Http | Https
  deriving (Eq,Ord,Show,Enum,Bounded)

toServantScheme :: Scheme -> S.Scheme
toServantScheme Http = S.Http
toServantScheme Https = S.Https

schemeToString :: Scheme -> String
schemeToString Http = "http"
schemeToString Https = "https"

data Url = Url
  { urlHost :: String
  , urlPort :: Int
  } deriving (Eq,Ord,Show)

urlToString :: Url -> String
urlToString (Url h p) = h <> ":" <> show p

urlParser :: String -> Int -> Parser Url
urlParser prefix defaultPort = Url
    <$> strOption (long (prefix <> "-host") <> metavar "HOST" <> help ("host for the " <> prefix <> " API"))
    <*> option auto (long (prefix <> "-port") <> metavar "PORT" <> value defaultPort <> help portHelp)
  where
    portHelp = printf "port for the %s API (default %d)" prefix defaultPort

schemeParser :: String -> Parser Scheme
schemeParser prefix =
  flag Http Https (long (prefix <> "-https") <> help "Use HTTPS to connect to the service API (instead of HTTP)")

data UrlScheme = UrlScheme
  { usScheme :: Scheme
  , usUrl :: Url
  } deriving (Eq, Show)

showUrlScheme :: UrlScheme -> String
showUrlScheme (UrlScheme s u) = schemeToString s <> "://" <> urlToString u

urlSchemeParser :: String -> Int -> Parser UrlScheme
urlSchemeParser prefix defaultPort = UrlScheme
  <$> schemeParser prefix
  <*> urlParser prefix defaultPort

newtype ChainwebVersion = ChainwebVersion { getCWVersion :: Text }
  deriving newtype (IsString, Eq, Show, Ord, Read)

newtype NodeDbPath = NodeDbPath { getNodeDbPath :: Maybe FilePath }
  deriving (Eq, Show)

readNodeDbPath :: ReadM NodeDbPath
readNodeDbPath = eitherReader $ \case
  "" -> Right $ NodeDbPath Nothing
  s -> Right $ NodeDbPath $ Just s

data Command
    = Server ServerEnv
    | Listen
    | Backfill BackfillArgs
    | Fill FillArgs
    | Single ChainId BlockHeight
    | FillEvents BackfillArgs EventType
    | BackFillTransfers Bool BackfillArgs
    deriving (Show)

data BackfillArgs = BackfillArgs
  { _backfillArgs_delayMicros :: Maybe Int
  , _backfillArgs_chunkSize :: Maybe Int
  } deriving (Eq,Ord,Show)

data FillArgs = FillArgs
  { _fillArgs_delayMicros :: Maybe Int
  , _fillArgs_disableIndexes :: Bool
  } deriving (Eq, Ord, Show)

data ServerEnv = ServerEnv
  { _serverEnv_port :: Int
  , _serverEnv_runFill :: Bool
  , _serverEnv_fillDelay :: Maybe Int
  } deriving (Eq,Ord,Show)

envP :: Parser Args
envP = Args
  <$> commands
  <*> (fromMaybe (PGGargoyle "cwdb-pgdata") <$> optional connectP)
  <*> urlSchemeParser "service" 1848
  <*> urlParser "p2p" 443
  <*> logLevelParser
  <*> migrationP

migrationP :: Parser MigrateStatus
migrationP =
  flag DontMigrate RunMigration (short 'm' <> long "migrate" <> help "Run DB migration")

logLevelParser :: Parser LogLevel
logLevelParser =
    option (eitherReader (readLogLevel . pack)) $
      long "level"
      <> value Info
      <> help "Initial log threshold"

richListP :: Parser Args
richListP = hsubparser
  ( command "richlist"
    ( info rlOpts
      ( progDesc "Create a richlist using existing chainweb node data"
      )
    )
  )
  where
    rlOpts = RichListArgs
      <$> option readNodeDbPath
        ( long "db-path"
        <> value (NodeDbPath Nothing)
        <> help "Chainweb node db filepath"
        )
      <*> logLevelParser
      <*> simpleVersionParser

versionReader :: ReadM ChainwebVersion
versionReader = eitherReader $ \case
  txt | map toLower txt == "mainnet01" || map toLower txt == "mainnet" -> Right "mainnet01"
  txt | map toLower txt == "testnet04" || map toLower txt == "testnet" -> Right "testnet04"
  txt -> Left $ printf "Can'txt read chainwebversion: got %" txt

simpleVersionParser :: Parser ChainwebVersion
simpleVersionParser =
  option versionReader $
    long "chainweb-version"
    <> value "mainnet01"
    <> help "Chainweb node version"

connectP :: Parser Connect
connectP = (PGString <$> pgstringP) <|> (PGInfo <$> connectInfoP) <|> (PGGargoyle <$> dbdirP)

dbdirP :: Parser FilePath
dbdirP = strOption (long "dbdir" <> help "Directory for self-run postgres")

pgstringP :: Parser ByteString
pgstringP = strOption (long "dbstring" <> help "Postgres Connection String")

-- | These defaults are pulled from the postgres-simple docs.
connectInfoP :: Parser ConnectInfo
connectInfoP = ConnectInfo
  <$> strOption   (long "dbhost" <> value "localhost" <> help "Postgres DB hostname")
  <*> option auto (long "dbport" <> value 5432        <> help "Postgres DB port")
  <*> strOption   (long "dbuser" <> value "postgres"  <> help "Postgres DB user")
  <*> strOption   (long "dbpass" <> value ""          <> help "Postgres DB password")
  <*> strOption   (long "dbname" <> help "Postgres DB name")

singleP :: Parser Command
singleP = Single
  <$> (ChainId <$> option auto (long "chain" <> metavar "INT"))
  <*> option auto (long "height" <> metavar "INT")

serverP :: Parser ServerEnv
serverP = ServerEnv
  <$> option auto (long "port" <> metavar "INT" <> help "Port the server will listen on")
  <*> flag False True (long "run-fill" <> short 'f' <> help "Run fill operation once a day to fill gaps")
  <*> delayP

delayP :: Parser (Maybe Int)
delayP = optional $ option auto (long "delay" <> metavar "DELAY_MICROS" <> help  "Number of microseconds to delay between queries to the node")

bfArgsP :: Parser BackfillArgs
bfArgsP = BackfillArgs
  <$> delayP
  <*> optional (option auto (long "chunk-size" <> metavar "CHUNK_SIZE" <> help "Number of transactions to query at a time"))

fillArgsP :: Parser FillArgs
fillArgsP = FillArgs
  <$> delayP
  <*> flag False True (long "disable-indexes" <> short 'd' <> help "Disable indexes on tables while filling.")

data EventType = CoinbaseAndTx | OnlyTx
  deriving (Eq,Ord,Show,Read,Enum,Bounded)

eventTypeP :: Parser EventType
eventTypeP =
  flag CoinbaseAndTx OnlyTx (long "only-tx" <> help "Only fill missing events associated with transactions")

commands :: Parser Command
commands = hsubparser
  (  command "listen" (info (pure Listen)
       (progDesc "Node Listener - Waits for new blocks and adds them to work queue"))
  <> command "backfill" (info (Backfill <$> bfArgsP)
       (progDesc "Backfill Worker - Backfills blocks from before DB was started (DEPRECATED)"))
  <> command "fill" (info (Fill <$> fillArgsP)
       (progDesc "Fills the DB with  missing blocks"))
  <> command "gaps" (info (Fill <$> fillArgsP)
       (progDesc "Gaps Worker - Fills in missing blocks lost during backfill or listen (DEPRECATED)"))
  <> command "single" (info singleP
       (progDesc "Single Worker - Lookup and write the blocks at a given chain/height"))
  <> command "server" (info (Server <$> serverP)
       (progDesc "Serve the chainweb-data REST API (also does listen)"))
  <> command "fill-events" (info (FillEvents <$> bfArgsP <*> eventTypeP)
       (progDesc "Event Worker - Fills missing events"))
  <> command "backfill-transfers" (info (BackFillTransfers <$> flag False True (long "disable-indexes" <> help "Delete indexes on transfers table") <*> bfArgsP)
       (progDesc "Backfill transfer table entries"))
  )

progress :: LogFunctionIO Text -> IORef Int -> Int -> IO a
progress logg count total = do
    start <- getPOSIXTime
    let go lastTime lastCount = do
          threadDelay 30_000_000  -- 30 seconds. TODO Make configurable?
          completed <- readIORef count
          now <- getPOSIXTime
          let perc = (100 * fromIntegral completed / fromIntegral total) :: Double
              elapsedSeconds = now - start
              elapsedSecondsSinceLast = now - lastTime
              instantBlocksPerSecond = (fromIntegral (completed - lastCount) / realToFrac elapsedSecondsSinceLast) :: Double
              totalBlocksPerSecond = (fromIntegral completed / realToFrac elapsedSeconds) :: Double
              estSecondsLeft = floor (fromIntegral (total - completed) / instantBlocksPerSecond) :: Int
              (timeUnits, timeLeft) | estSecondsLeft < 60 = ("seconds" :: String, estSecondsLeft)
                                    | estSecondsLeft < 3600 = ("minutes" :: String, estSecondsLeft `div` 60)
                                    | otherwise = ("hours", estSecondsLeft `div` 3600)
          logg Info $ fromString $ printf "Progress: %d/%d (%.2f%%), ~%d %s remaining at %.0f current items per second (%.0f overall average)."
            completed total perc timeLeft timeUnits instantBlocksPerSecond totalBlocksPerSecond
          hFlush stdout
          go now completed
    go start 0
