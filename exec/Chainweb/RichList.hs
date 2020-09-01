{-# LANGUAGE OverloadedStrings #-}
module Chainweb.RichList
( richList
) where


import Control.Scheduler (Comp(Par'), traverseConcurrently_)

import Chainweb.Api.ChainId
import Chainweb.Api.NodeInfo
import Chainweb.Env
import Chainweb.Lookups

import System.Process

import Text.Printf

{-


for i in {0..19}
do
   sqlite3 -header -csv pact-v1-chain-$i.sqlite "select $i as chain, rowkey as acct_id, txid, cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) as 'balance'
   from 'coin_coin-table' as coin
   INNER JOIN (
    select
     rowkey as acct_id,
     max(txid) as last_txid
    from 'coin_coin-table'
    group by acct_id
   ) latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid
   order by balance desc;" >> chain$i.csv


-}
richList :: Env -> IO ()
richList cenv = do
    cut <- queryCut cenv

    let bh = fromIntegral $ cutMaxHeight cut
        cids = atBlockHeight bh chains

    traverseConcurrently_ Par' enrich cids
  where
    chains = _env_chainsAtHeight cenv

enrich :: ChainId -> IO ()
enrich (ChainId c) = do
    printf "[INFO] Compiling rich-list for chain id %d" c
    callCommand cmd
  where
    cmd = printf
      "sqlite3 -header -csv pact-v1-chain-%d.sqlite \"select %d as chain, rowkey as acct_id, txid, \
      \cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) \
      \as 'balance' from 'coin_coin-table' as coin INNER JOIN \
      \(select rowkey as acct_id, max(txid) as last_txid from 'coin_coin-table' group by acct_id) \
      \latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid order by balance desc;\" \
      \>> richlist-v1-chain-%d.csv" c
