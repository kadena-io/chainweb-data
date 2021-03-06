#!/bin/sh


for i in $(seq 0 $1);
do
   sqlite3 -header -csv pact-v1-chain-$i.sqlite "select rowkey as acct_id, txid, cast(ifnull(json_extract(rowdata, '$.balance.decimal'), json_extract(rowdata, '$.balance')) as REAL) as 'balance'
     from [coin_coin-table] as coin
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
rm pact-v1-chain-*.sqlite*
