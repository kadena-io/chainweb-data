{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
module Chainweb.Env
  ( Args(..)
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
  , envP
  , richListP
  , NodeDbPath(..)
  ) where

import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Api.NodeInfo
import           Control.Concurrent
import           Control.Exception
import           Data.ByteString (ByteString)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe
import           Data.String
import           Data.Pool
import           Data.Text (Text)
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

---

data Args
  = Args Command Connect UrlScheme Url
    -- ^ arguments for the Listen, Backfill, Gaps, Single,
    -- and Server cmds
  | RichListArgs NodeDbPath
    -- ^ arguments for the Richlist command
  deriving (Show)

data Env = Env
  { _env_httpManager :: Manager
  , _env_dbConnPool :: Pool Connection
  , _env_serviceUrlScheme :: UrlScheme
  , _env_p2pUrl :: Url
  , _env_nodeInfo :: NodeInfo
  , _env_chainsAtHeight :: [(BlockHeight, [ChainId])]
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
    pool <- createPool (connectPostgreSQL dbUri) close 1 5 caps
    func pool

-- | Create a `Pool` based on `Connect` settings designated on the command line.
getPool :: IO Connection -> IO (Pool Connection)
getPool getConn = do
  caps <- getNumCapabilities
  createPool getConn close 1 5 caps

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

parseUrl :: String -> Url
parseUrl s = Url h (read $ drop 1 pstr)-- Read should be ok here because it's run on startup
  where
    (h,pstr) = break (==':') s

urlParser :: String -> Parser Url
urlParser prefix = parseUrl <$> strOption (long (prefix <> "-url") <> metavar "URL" <> help "url:port of Chainweb node")

schemeParser :: Parser Scheme
schemeParser =
  flag Http Https (long "use-https" <> help "Use HTTPS to connect to the node")

data UrlScheme = UrlScheme
  { usScheme :: Scheme
  , usUrl :: Url
  } deriving (Eq, Show)

showUrlScheme :: UrlScheme -> String
showUrlScheme (UrlScheme s u) = schemeToString s <> "://" <> urlToString u

urlSchemeParser :: String -> Parser UrlScheme
urlSchemeParser prefix = UrlScheme <$> schemeParser <*> urlParser prefix

newtype ChainwebVersion = ChainwebVersion Text
  deriving newtype (IsString)

newtype NodeDbPath = NodeDbPath { getNodeDbPath :: Maybe FilePath }
  deriving (Eq, Show)

readNodeDbPath :: ReadM NodeDbPath
readNodeDbPath = eitherReader $ \case
  "" -> Right $ NodeDbPath Nothing
  s -> Right $ NodeDbPath $ Just s

data Command
    = Server ServerEnv
    | Listen
    | Backfill
    | Gaps
    | Single ChainId BlockHeight
    deriving (Show)

data ServerEnv = ServerEnv
  { _serverEnv_port :: Int
  , _serverEnv_verbose :: Bool
  } deriving (Eq,Ord,Show)

envP :: Parser Args
envP = Args
  <$> commands
  <*> (fromMaybe (PGGargoyle "cwdb-pgdata") <$> optional connectP)
  <*> urlSchemeParser "service"
  <*> urlParser "p2p"

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
  <*> switch (short 'v' <> help "Verbose mode that shows when headers come in")

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
  <> command "server" (info (Server <$> serverP)
       (progDesc "Serve the chainweb-data REST API (also does listen)"))
  )
