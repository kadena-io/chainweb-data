{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
module Chainweb.RichList
( richList
) where


import Control.DeepSeq
import Control.Monad
import Control.Scheduler

import Chainweb.Api.ChainId
import Chainweb.Api.NodeInfo
import Chainweb.Env
import Chainweb.Lookups

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Csv as Csv
import Data.Pool
import qualified Data.Text as T
import qualified Data.Vector as V

import Database.Beam.Postgres (Connection)

import GHC.Generics

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
    void $! createProcess_ ("rich-list-" <> show cid) (shell cmd) { delegate_ctlc = True }
  where
    c = T.pack $ show cid

    cmd = T.unpack
      [text|
       sqlite3 -header -csv ~/.local/share/chainweb-node/mainnet01/0/sqlite/pact-v1-chain-$c.sqlite
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
consolidate :: Pool Connection -> [ChainId] -> IO ()
consolidate _pool cids = do
    entries <- foldM extractCsv [] cids
    print entries
  where
    extractCsv lst (ChainId c) = do
      let richCsv = "rich-list-chain-" <> show c <> ".csv"

      csvData <- LBS.readFile richCsv

      case Csv.decode Csv.NoHeader csvData of
        Left e -> error $ "Error while decoding richlist: " <> show e
        Right rs -> return $ V.foldl' (\acc (RichAccount a b) -> (a,b):acc) lst rs

-- -------------------------------------------------------------------- --
-- Local data

data RichAccount = RichAccount
    { richAccount :: !String
    , richBalanace :: !Double
    } deriving
      ( Eq, Ord, Show
      , Generic, NFData
      , Csv.ToRecord, Csv.FromRecord
      )
