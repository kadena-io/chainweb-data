module ChainwebDb.Migration where

import qualified Crypto.Hash.MD5 as MD5 (hash)

import BasePrelude (exitFailure)

import Control.Monad

import qualified Data.ByteString as BS
import qualified Data.Map as Map
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
matchSteps stepsIn existingIn = do
    orderedSteps <- do
      orderTagged <- forM stepsIn $ \ms -> do
        (order, _) <- parseScriptName $ msName ms
        return (order, pure ms)
      sequence $ Map.elems $ flip Map.fromListWithKey orderTagged $
        \order left' right' -> do
          left <- left'
          right <- right'
          Left $ "Duplicate step order: " <> show order <> " for steps "
              <> show (msName left) <> " and " <> show (msName right)
    matchRecursive orderedSteps existingIn
  where
    matchRecursive steps [] = Right steps
    matchRecursive [] (sm:_) = Left $
      "Extra steps in existing migrations: " <> show (Mg.schemaMigrationName sm) <> "..."
    matchRecursive (ms:steps) (sm:rest) = do
      (thisOrder, thisName) <- parseScriptName $ msName ms
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
      matchRecursive steps rest

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
