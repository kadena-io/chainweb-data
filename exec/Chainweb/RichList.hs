{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
module Chainweb.RichList
( richList
) where


import Control.Scheduler (Comp(Par'), traverseConcurrently_)

import Chainweb.Api.ChainId
import Chainweb.Api.NodeInfo
import Chainweb.Env
import Chainweb.Lookups

import qualified Data.Text as T

import NeatInterpolation

import System.Process

import Text.Printf


richList :: Env -> IO ()
richList cenv = do
    cut <- queryCut cenv

    let bh = fromIntegral $ cutMaxHeight cut
        cids = atBlockHeight bh chains

    traverseConcurrently_ Par' enrich cids
    consolidate pool cids
  where
    pool = _env_dbConnPool cenv
    chains = _env_chainsAtHeight cenv

-- | For each chain id, generate a rich list csv containing a sorted list of
-- accounts and their balances
--
-- Invariant: 'pact-v1-chain-$c.sqlite' will always exist because
-- we are dispatching on latest cut height, which is one of: 10, or 20.
--
enrich :: ChainId -> IO ()
enrich (ChainId cid) = do
    printf "[INFO] Compiling rich-list for chain id %d" cid
    callCommand cmd
  where
    c = T.pack $ show cid

    cmd = T.unpack
      [text|
       sqlite3 -header -csv pact-v1-chain-$c.sqlite
         "select rowkey as acct_id, txid, cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) as 'balance'
         from 'coin_coin-table' as coin
         INNER JOIN (
          select
           rowkey as acct_id,
           max(txid) as last_txid
          from 'coin_coin-table'
          group by acct_id
         ) latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid
         order by balance desc;" > rich-list-chain-$c.csv
      |]

-- | TODO: write to postgres. In the meantime, testing with print statements
--
consolidate :: Pool -> [ChainId] -> IO ()
consolidate pool cids = do
    entries <- foldM extractCsv [] cids
    print entries
  where
    extractCsv (ChainId c) lst = do
      let richCsv = "rich-list-chain-" <> show c <> ".csv"

      csvData <- LBS.readFile richCsv

      case Csv.decode NoHeader csvData of
        Left e -> throwIO $ "Error while decoding richlist: " <> show e
        Right rs -> V.foldl' (\acc (Account a b) -> (a,b):acc) lst rs
