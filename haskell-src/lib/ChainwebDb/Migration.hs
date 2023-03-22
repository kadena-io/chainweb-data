module ChainwebDb.Migration where

import qualified Crypto.Hash.MD5 as MD5 (hash)

import BasePrelude (exitFailure)

import Control.Monad

import qualified Data.ByteString as BS
import qualified Data.Pool as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Database.PostgreSQL.Simple
import qualified Database.PostgreSQL.Simple.Migration as Mg

import Safe (readMay)
import System.Logger hiding (logg)

newtype MigrationOrder = MigrationOrder [Int] deriving (Eq, Ord, Show)

type StepName = String

data MigrationStep = MigrationStep
  { msName :: Mg.ScriptName
  , msBody :: BS.ByteString
  }

-- | Parse a ScriptName in the format "1.2.3_step_name" into a MigrationOrder
-- and a StepName.
parseScriptName :: Mg.ScriptName -> Either String (MigrationOrder, StepName)
parseScriptName fullName = do
  (orderStr, name) <- case break (== '_') fullName of
    (_, "") -> Left $ "Could not find _ in " <> fullName
    (a, _:b) -> Right (a, b)

  order <- parseOrder orderStr
  return (MigrationOrder order, name)
  where
    parseOrder :: String -> Either String [Int]
    parseOrder = mapM readInt . splitOn '.'
    readInt :: String -> Either String Int
    readInt s = case readMay s of
      Just i -> Right i
      Nothing -> Left $ "Could not parse int from " <> s
    splitOn :: Char -> String -> [String]
    splitOn c s = case break (== c) s of
      (a, "") -> [a]
      (a, _:b) -> a : splitOn c b

-- Given a list of steps to execute and a list of steps that have already been
-- executed check that the sequence of steps satisfies the following:
-- 1. The steps that have been executed are a prefix of the steps to execute
-- 2. The steps to execute are in order
-- 3. The checksums of the steps to execute match the checksums of the steps
matchSteps ::
  [MigrationStep] ->
  [Mg.SchemaMigration] ->
  Either String [MigrationStep]
matchSteps = matchRecursive Nothing
  where
    matchRecursive _ [] [] = Right []
    matchRecursive _ [] (sm:_) = Left $
      "Extra steps in existing migrations: " <> show (Mg.schemaMigrationName sm) <> "..."
    matchRecursive mbPrevOrder (ms:steps) existing = do
      (thisOrder, thisName) <- parseScriptName $ msName ms
      forM_ mbPrevOrder $ \prevOrder -> do
        unless (prevOrder < thisOrder) $ Left $ "Steps out of order: " <> thisName
      rest <- case existing of
        [] -> return []
        (sm:rest) -> do
          (order, name) <- parseScriptName $ T.unpack $ T.decodeUtf8 $ Mg.schemaMigrationName sm
          unless (thisOrder == order) $ Left
            $ "Steps out of order: Wanted step " <> show thisName
           <> " has order " <> show thisOrder <> " but found step " <> show name
           <> " with order " <> show order
          unless (thisName == name) $ Left
            $ "Steps out of order: Wanted step " <> show thisName
           <> " but found step " <> show name
          let wantedChecksum = MD5.hash $ msBody ms
              foundChecksum = Mg.schemaMigrationChecksum sm
          unless (wantedChecksum == foundChecksum) $ Left
            $ "Checksum mismatch: Wanted step " <> show thisName
           <> " with checksum " <> show wantedChecksum <> " but found step " <> show name
           <> " with checksum " <> show foundChecksum
          return rest
      matchRecursive (Just thisOrder) steps rest

data MigrationAction
  = RunMigrations
  | CheckMigrations
  | PrintMigrations
  deriving (Eq,Ord,Show,Read)

runMigrations ::
  MigrationAction ->
  [MigrationStep] ->
  P.Pool Connection ->
  LogFunctionIO T.Text ->
  IO ()
runMigrations act steps pool logg = P.withResource pool $ \c -> withTransaction c $ do
  existing <- Mg.getMigrations c
  case matchSteps steps existing of
    Left err -> do
      let errLog = flip logg $ "Migration error: " <> T.pack err
      case act of
        RunMigrations -> errLog Error >> exitFailure
        CheckMigrations -> errLog Error >> exitFailure
        PrintMigrations -> errLog Info
    Right [] ->
      logg Info "No migrations to run"
    Right steps' -> case act of
      RunMigrations -> forM_ steps' $ \ms -> do
        logg Info $ "Running migration: " <> T.pack (msName ms)
        logg Debug $ "Migration body: " <> T.decodeUtf8 (msBody ms)
        Mg.runMigrations False c [Mg.MigrationScript (msName ms) (msBody ms)]
      CheckMigrations -> missingMigrations True
      PrintMigrations -> missingMigrations False
      where
        missingMigrations isFatal = do
          let errLog = flip logg
                $ "Missing migration steps: "
               <> T.intercalate ", " (T.pack . msName <$> steps')
          if isFatal
          then errLog Error >> exitFailure
          else errLog Info
