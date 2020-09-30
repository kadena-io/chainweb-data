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
import Data.List (sortOn, isPrefixOf, sort)
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

      cids <- doesPathExist sqlitePath >>= \case
        True -> do
          dir <- filter ((==) ".sqlite" . takeExtension) <$> listDirectory sqlitePath

          -- count the number of  and aggregate associate file paths
          --
          let f (ns,acc) p
                | "pact-v1-chain-" `isPrefixOf` p =
                  let (p',_) = splitExtension p
                      -- this is not a magical 14 - this is the number of chars in "pact-v1-chain"
                      cid = read @Int $ snd $ splitAt 14 p'
                  in (cid:ns, p:acc)
                | otherwise = (sort ns,acc)

          let (chains, files) = foldl' f mempty dir
              isConsecutive = all (\(x,y) -> succ x == y) . (zip <*> tail)

          unless (isConsecutive chains)
            $ ioError $ userError
            $ "Missing tables for some chain ids. Is your node synced?"

          -- copy all files to current working dir
          traverse_ (\p -> copyFile (sqlitePath </> p) p) files
          return $ length chains
        False -> ioError $ userError $ "Cannot find sqlite data. Is your node synced?"

      -- return # of chains for bash
      return cids

    go
      :: M.Map String Double
      -> (String, String, String)
      -> M.Map String Double
    go acc (acct,_,bal)
      | bal == "balance" = acc
      | otherwise = M.insertWith (+) acct (read @Double bal) acc
