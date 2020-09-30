{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Chainweb.RichList
( richList
) where


import Control.Monad

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Csv as Csv
import Data.Foldable (traverse_, foldl')
import Data.List (sortOn, isPrefixOf)
import qualified Data.Map.Strict as M
import Data.Ord (Down(..))
import qualified Data.Vector as V

import System.Directory
import System.FilePath
import System.Process


richList :: FilePath -> IO ()
richList fp = do
    --
    -- Steps:
    --   1. Check whether specified top-level db path is reachable
    --   2. We assume the node data db is up to date, and for chains 0..19,
    --      Check that the sqlite db paths exist. If yes, copy them to current
    --      working dir.
    --   3. Execute richlist generation, outputing `richlist.csv`
    --   4. Aggregate richest accounts and prune to top 100
    --
    chains <- doesPathExist fp >>= \case
      True -> copyTables
      False -> ioError $ userError
        $ "Chainweb-node top-level db directory does not exist: "
        <> fp

    putStrLn "[INFO] Aggregating richlist.csv..."
    let cmd = proc "/bin/sh" ["scripts/richlist.sh", show chains]
    void $! readCreateProcess cmd []

    putStrLn "[INFO] Filtering top 100 richest accounts..."
    void $! pruneRichList

    putStrLn "[INFO] Finished."
  where
    pruneRichList = do
      csv <- LBS.readFile "richlist.csv"
      case Csv.decode Csv.HasHeader csv of
        Left e -> ioError $ userError $ "Could not decode rich list .csv file: " <> e
        Right rs -> do
          let acc = Csv.encode
                $ take 100
                $ sortOn (Down . snd)
                $ M.toList
                $ V.foldl' go M.empty rs

          void $! LBS.writeFile "richlist.csv" acc

    copyTables :: IO Int
    copyTables = do
      let prefix = "chainweb-node/mainnet01/0/sqlite"
          sqlitePath = fp </> prefix

      (chains, files) <- doesPathExist sqlitePath >>= \case
        True -> do
          dir <- filter ((==) ".sqlite" . takeExtension) <$> listDirectory sqlitePath
          let f (n,acc) p
                | (sqlitePath </> "pact-v1-chain-") `isPrefixOf` p = (n+1,(sqlitePath </> p):acc)
                | otherwise = (n,acc)
          return $ foldl' f (0, []) dir
        False -> ioError $ userError $ "Cannot find sqlite data. Is your node synced?"

      print files
      print chains
      traverse_ (flip copyFile ".") files
      return chains

    go
      :: M.Map String Double
      -> (String, String, String)
      -> M.Map String Double
    go acc (acct,_,bal)
      | bal == "balance" = acc
      | otherwise = M.insertWith (+) acct (read @Double bal) acc
