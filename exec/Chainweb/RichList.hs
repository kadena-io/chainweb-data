{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}
module Chainweb.RichList ( richList ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Csv as Csv
import Data.Foldable (foldrM)
import Data.List (isPrefixOf, sort,sortOn)
import qualified Data.Map.Strict as M
import Data.Ord (Down(..))
import qualified Data.Text as T
import Data.Text (Text)
import Data.String.Conv
import Data.String

import System.Directory
import System.FilePath
import System.Logger.Types

import Text.Read

import Database.SQLite3
import Database.SQLite3.Direct (Utf8(..))

import Pact.Types.SQLite


richList :: LogFunctionIO Text -> FilePath -> IO ()
richList logger fp = do

    files <- doesPathExist fp >>= \case
        True -> checkChains
        False -> ioError $ userError
          $ "Chainweb-node top-level db directory does not exist: "
          <> fp
    logger Info "Aggregating richlist ..."
    results <- foldrM (\file acc -> (acc .) <$> withSQLiteConnection file richListQuery) id files
    logger Info $ "Filterting top 100 richest accounts..."
    pruneRichList (results [])
  where
    checkChains :: IO [FilePath]
    checkChains = do
        let sqlitePath = fp <> "chainweb-node/mainnet01/0/sqlite" -- TODO: notice this only works for mainnet

        doesPathExist sqlitePath >>= \case
          False -> ioError $ userError "Cannot find sqlite data. Is your node synced?"
          True -> do
            files <- filter ((==) ".sqlite" . takeExtension) <$> listDirectory sqlitePath

            let go p
                  | "pact-v1-chain-" `isPrefixOf` p =
                    case splitAt 14 (fst $ splitExtension p) of
                      (_, "") -> error $ "Found corrupt sqlite path: " <> p
                      (_, cid) -> case readMaybe @Int cid of
                        Just c -> ((p :), (c :))
                        Nothing -> error "Couldn't read chain id"
                  | otherwise = mempty
                (fdl, cdl) = foldMap go files
                chains = cdl []
                isConsecutive = all (\(x,y) -> succ x == y)
                  . (zip <*> tail)
                  . sort
            unless (isConsecutive chains)
              $ ioError $ userError
              $ "Missing tables for some chain ids. Is your node synced?"
            return $ fdl []

pruneRichList :: [(Text,Double)] -> IO ()
pruneRichList = LBS.writeFile "richlist.csv"
    . Csv.encode
    . take 100
    . sortOn (Down . snd)
    . M.toList
    . M.fromListWith (+)

-- WARNING: This function will throw errors if found. We don't "catch" errors in an Either type
withSQLiteConnection :: FilePath -> (Database -> IO a) -> IO a
withSQLiteConnection fp action = bracket (open (T.pack fp)) close action

type DList a = [a] -> [a]

richListQuery :: Database -> IO (DList (Text, Double))
richListQuery db = do
    rows <- qry_ db (Utf8 richListQueryStmt) [RText, RInt, RDouble]
    return $ (`foldMap` rows) $ \case
      [SText (Utf8 account), SInt _txid, SDouble balance] -> ((toS account,balance) :)
      _ -> error "impossible?" -- TODO: Make this use throwError/throwM instead of die

richListQueryStmt :: IsString s => s
richListQueryStmt =
   "select rowkey as acct_id, txid, cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) as 'balance'\
     \ from [coin_coin-table] as coin\
     \ INNER JOIN (\
      \ select\
       \ rowkey as acct_id,\
       \ max(txid) as last_txid\
      \ from 'coin_coin-table'\
      \ group by acct_id\
     \ ) latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid\
     \ order by balance desc;"
