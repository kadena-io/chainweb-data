{-# language LambdaCase #-}
module Chainweb.RichListScript where

import Control.Exception
import Control.Monad (forM)
import qualified Data.Text as T
import Data.Text (Text)
import Data.String.Conv
import Data.String
import System.Exit (die)

import Database.SQLite3
import Database.SQLite3.Direct (Utf8(..))

import Pact.Types.SQLite

-- WARNING: This function will throw errors if found. We don't "catch" errors in an Either type
withSQLiteConnection :: FilePath -> (Database -> IO a) -> IO a
withSQLiteConnection fp action = bracket (open (T.pack fp)) close action

richListQuery :: Database -> IO [(Text, Double)]
richListQuery db = do
    rows <- qry_ db (Utf8 richListQueryStmt) [RText, RInt, RDouble]
    forM rows $ \case
      [SText (Utf8 account), SInt _txid, SDouble balance] -> pure (toS account,balance)
      _ -> die "impossible?" -- TODO: Make this use throwError/throwM instead of die

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
