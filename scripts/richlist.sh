#!/bin/sh

DIR=$1;

for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
do
   DB=$DIR/chainweb-node/mainnet01/0/sqlite/pact-v1-chain-$i.sqlite

   sqlite3 -header -csv $DB "select rowkey as acct_id, txid, cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) as 'balance'
     from 'coin_coin-table' as coin
     INNER JOIN (
      select
       rowkey as acct_id,
       max(txid) as last_txid
      from 'coin_coin-table'
      group by acct_id
     ) latest ON coin.rowkey = latest.acct_id AND coin.txid = latest.last_txid
     order by balance desc;" > richlist-chain-$i.csv
done


cat richlist-chain-*.csv > richlist.csv
rm richlist-chain-*.csv
