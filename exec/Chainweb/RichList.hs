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
import Data.Foldable (for_)
import Data.List (sortOn)
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
    void $! doesPathExist fp >>= \case
      True -> for_ [0::Int .. 19] $ \i -> do
        let prefix = "chainweb-node/mainnet01/0/sqlite"
            postfix = "pact-v1-chain-" <> show i <> ".sqlite"
        let fp' = fp </> prefix </> postfix

        doesFileExist fp' >>= \case
          True -> void $! copyFile fp' postfix
          False -> ioError $ userError $ "Cannot find sqlite table: " <> fp' <> ". Is your node synced?"
      False -> ioError $ userError $ "Chainweb-node top-level db directory does not exist: " <> fp

    let cmd = proc "/bin/sh" ["scripts/richlist.sh"]

    putStrLn "[INFO] Aggregating richlist.csv..."
    void $! readCreateProcess cmd []

    putStrLn "[INFO] Filtering top 100 richest accounts..."
    void $! pruneRichList
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

    go
      :: M.Map String Double
      -> (String, String, String)
      -> M.Map String Double
    go acc (acct,_,bal)
      | bal == "balance" = acc
      | otherwise = M.insertWith (+) acct (read @Double bal) acc
