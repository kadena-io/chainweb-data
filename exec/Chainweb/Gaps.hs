{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Gaps ( gaps ) where

import           BasePrelude
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Common (BlockHeight)
import           Chainweb.Env
import           Chainweb.Lookups
import           Chainweb.Worker (writeBlock)
import           ChainwebData.Types
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import qualified Data.Pool as P
import           Database.Beam.Postgres
import           Database.PostgreSQL.Simple
---

gaps :: Env -> Maybe Int -> IO ()
gaps e delay = do
  withGaps pool $ \(cid,lo,hi) -> do
      count <- newIORef 0
      let strat = case delay of
            Nothing -> Par'
            Just _ -> Seq
      traverseConcurrently_ strat (f count cid) [lo + 1, lo + 2 .. hi - 1]
      final <- readIORef count
      printf "[INFO] Filled in %d missing blocks.\n" final
  where
    pool = _env_dbConnPool e
    f :: IORef Int -> Int -> BlockHeight -> IO ()
    f count cid h = do
      headersBetween e (ChainId cid, Low h, High h) >>= traverse_ (writeBlock e pool count)
      forM_ delay threadDelay

withGaps :: P.Pool Connection -> ((Int, BlockHeight, BlockHeight) -> IO ()) -> IO ()
withGaps pool f = P.withResource pool $ \c ->
    join $ fold_
      c
      queryString
      (printf "[INFO] No gaps detected.\n")
      (\_ x -> pure $ f x)
  where
    queryString =
        "with gaps as \
        \(select chainid, height, LEAD (height,1) \
        \OVER (PARTITION BY chainid ORDER BY height ASC) \
        \AS next_height from blocks group by chainid, height) \
        \select * from gaps where next_height - height > 1;"

