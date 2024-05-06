{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
module Chainweb.RichList ( richList ) where

import Control.Applicative ((<|>))
import Control.Monad
import Control.Lens
import Data.Aeson (eitherDecodeStrict, Value(..))
import Data.Aeson.Lens
import qualified Data.ByteString.Lazy as LBS
import Data.ByteString (ByteString)
import qualified Data.Csv as Csv
import Data.Foldable (asum)
import Data.Int (Int64)
import Data.List (isPrefixOf, sort,sortOn)
import qualified Data.Map.Strict as M
import Data.Ord (Down(..))
import qualified Data.Text as T
import Data.Text (Text)
import Data.Text.Read (double)
import Data.String.Conv

import System.Directory
import System.FilePath
import System.Logger.Types

import Text.Printf (printf)
import Text.Read

import Database.SQLite.Simple

import ChainwebData.Env (ChainwebVersion(..))

richList :: LogFunctionIO Text -> FilePath -> ChainwebVersion -> IO ()
richList logger fp (ChainwebVersion version) = do

    files <- doesPathExist fp >>= \case
        True -> checkChains
        False -> ioError $ userError
          $ "Chainweb-node top-level db directory does not exist: "
          <> fp
    logger Info "Aggregating richlist ..."
    results <- fmap mconcat $ forM files $ \(cid, file) -> fmap (fmap (\(acct,txid, bal) -> (cid,acct,txid,bal))) $ withConnection file richListQuery
    logger Info $ "Filtering top 100 richest accounts..."
    pruneRichList (either error id . parseResult <$> results)
  where
    parseResult (cid, a,txid, b) = do
      validJSON <- eitherDecodeStrict b
      let msg = "Unable to get balance\n invalid JSON " <> show validJSON
      maybe (Left msg) (Right . (cid,a,txid,)) $ getBalance validJSON
    checkChains :: IO [(Int, FilePath)]
    checkChains = do
        let sqlitePath = appendSlash fp <> "chainweb-node/" <> T.unpack version <> "/0/sqlite"
            appendSlash str = if last str == '/' then str else str <> "/"

        doesPathExist sqlitePath >>= \case
          False -> ioError $ userError $ printf "Cannot find sqlite data (at \"%s\"). Is your node synced?" sqlitePath
          True -> do
            files <- filter ((==) ".sqlite" . takeExtension) <$> listDirectory sqlitePath

            let go p
                  | "pact-v1-chain-" `isPrefixOf` p =
                    case splitAt 14 (fst $ splitExtension p) of
                      (_, "") -> error $ "Found corrupt sqlite path: " <> p
                      (_, cid) -> case readMaybe @Int cid of
                        Just c -> (((sqlitePath <> "/" <> p) :), (c :))
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
            return $ zip (cdl []) $ fdl []

getBalance :: Value -> Maybe Double
getBalance bytes = asum $ basecase : (fmap getBalance $ bytes ^.. members)
  where
    fromSci = fromRational . toRational
    basecase =
      bytes ^? key "balance" . _Number . to fromSci
      <|>
      bytes ^? key "balance" . key "decimal" . _Number . to fromSci
      <|>
      bytes ^? key "balance" . key "int" . _Number . to fromSci
      <|>
      bytes ^? key "balance" . key "decimal" . _String . to double . _Right . _1
      <|>
      bytes ^? key "balance" . key "int" . _String . to double . _Right . _1

pruneRichList :: [(Int, Text,Int64, Double)] -> IO ()
pruneRichList = LBS.writeFile "richlist.csv"
    . Csv.encode
    . take 100
    . map (\((cid,acct,txid),bal) -> (cid,acct,bal,txid))
    . sortOn (Down . snd)
    . M.toList
    . M.fromListWith (+)
    . map (\(cid,acct,txid, balance) -> ((cid,acct,txid), balance))

richListQuery :: Connection -> IO [(Text, Int64, ByteString)]
richListQuery conn = do
    rows <- query_ conn richListQueryStmt
    forM rows $ \(account, txid, jsonvalue) -> return (toS @ByteString @Text account,txid, jsonvalue)

richListQueryStmt :: Query
richListQueryStmt =
   "select rowkey as acct_id, txid, rowdata \
     \ from [coin_coin-table] as coin\
     \ INNER JOIN (\
      \ select\
       \ rowkey as acct_id,\
       \ max(txid) as last_txid\
      \ from 'coin_coin-table'\
      \ group by acct_id\
     \ ) latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid;"
