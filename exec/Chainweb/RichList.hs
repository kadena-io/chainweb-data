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
import Data.Foldable (traverse_)
import Data.List (sortOn, isPrefixOf, sort)
import qualified Data.Map.Strict as M
import           Data.Text (Text)
import Data.Ord (Down(..))
import qualified Data.Vector as V

import System.Directory
import System.FilePath
import System.Process
import System.Logger.Types
import Text.Read (readMaybe)
import Text.Printf (printf)


richList :: LogFunctionIO Text -> FilePath -> IO ()
richList logger fp = do
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

    logger Info $ "Aggregating richlist.csv..."
    let cmd = proc "/bin/sh" ["scripts/richlist.sh", show chains]
    void $! readCreateProcess cmd []

    logger Info $ "Filtering top 100 richest accounts..."
    void $! pruneRichList

    logger Info $ "Finished."
  where
    pruneRichList = do
      csv <- LBS.readFile "richlist.csv"
      case Csv.decode Csv.HasHeader csv of
        Left e -> ioError $ userError $ "Could not decode rich list .csv file: " <> e
        Right (rs :: V.Vector (String,String,String)) -> do
          let go acc (acct,_,bal)
                | bal == "balance" = acc
                -- | otherwise = M.insertWith (+) acct (read @Double bal) acc
                | otherwise = let err = error $ printf "richList: Couldn't read balance for account %s" acct
                in M.insertWith (+) acct (maybe err id $ readMaybe @Double bal) acc

          let acc = Csv.encode
                $ take 100
                $ sortOn (Down . snd)
                $ M.toList
                $ V.foldl' go M.empty rs

          void $! LBS.writeFile "richlist.csv" acc

    copyTables :: IO Int
    copyTables = do
      let sqlitePath = fp </> "chainweb-node/mainnet01/0/sqlite"

      doesPathExist sqlitePath >>= \case
        False -> ioError $ userError $ "Cannot find sqlite data. Is your node synced?"
        True -> do
          dir <- filter ((==) ".sqlite" . takeExtension) <$> listDirectory sqlitePath

          -- count the number of sqlite files and aggregate associated file paths
          --
          let f (ns,acc) p
                | "pact-v1-chain-" `isPrefixOf` p =
                  -- this is not a magical 14 - this is the number of chars in "pact-v1-chain-"
                  case splitAt 14 (fst $ splitExtension p) of
                    (_, "") -> ioError $ userError $ "Found corrupt sqlite path: " <> p
                    (_, cid) -> return ((read @Int cid):ns,p:acc)
                | otherwise = return (ns,acc)

          (chains, files) <- foldM f mempty dir

          let isConsecutive = all (\(x,y) -> succ x == y)
                . (zip <*> tail)
                . sort

          unless (isConsecutive chains)
            $ ioError $ userError
            $ "Missing tables for some chain ids. Is your node synced?"

          -- copy all files to current working dir
          traverse_ (\p -> copyFile (sqlitePath </> p) p) files

          -- return # of chains for bash
          return $ length chains
