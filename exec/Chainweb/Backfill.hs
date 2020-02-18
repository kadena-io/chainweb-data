{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Chainweb.Backfill ( backfill ) where

import           BasePrelude hiding (insert)
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Types
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.DbHash
import           ChainwebDb.Types.Header
import           Control.Concurrent.Async
import           Control.Concurrent.STM.TBQueue
import           Control.Error.Util (hush)
import           Data.Serialize (runGetLazy)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Tuple.Strict (T2(..))
import           Database.Beam
import           Database.Beam.Postgres (Connection, runBeamPostgres)
import           Network.HTTP.Client hiding (Proxy)

---

newtype Parent = Parent DbHash

{-

1. Get a confirmed block from the database.
2. Is its parent also in the database, or least in the work queue?
3. If yes, move to the next confirmed block.
4. If no, fetch the Header from the Node and write it to the queue.
5. Recurse on (2) with the parent of the Header we just wrote.

-}

backfill :: Env -> IO ()
backfill e@(Env _ c _ _) = do
  bs <- runBeamPostgres c . runSelectReturningList . select . all_ $ blocks database
  toCheck <- newTBQueueIO 1000
  toLook  <- newTBQueueIO 1000
  void . runConcurrently $ (,,)
    <$> Concurrently (traverse_ (filling toCheck) bs)
    <*> Concurrently (checking c toCheck toLook)
    <*> Concurrently (looking e toCheck toLook)

-- TODO Order by block creation time. That should ensure that the chains are
-- mixed up when reading blocks linearly from a SELECT.

-- TODO There is an occasional STM block here somewhere.

-- | Fill up the `toCheck` queue with entries from the DB. This is intended to
-- be ran in a single thread.
filling :: TBQueue (T2 ChainId Parent) -> Block -> IO ()
filling toCheck b = atomically . writeTBQueue toCheck
  $ T2 (ChainId $ _block_chainId b) (Parent $ _block_parent b)

-- | Add new parent hashes to an in-memory work queue if we've determined that
-- we don't already know about them.
--
-- This is intended to be ran in a single thread.
checking :: Connection -> TBQueue (T2 ChainId Parent) -> TBQueue (T2 ChainId Parent) -> IO ()
checking c toCheck toLook = forever $ do
  b@(T2 _ (Parent p)) <- atomically $ readTBQueue toCheck
  m <- runBeamPostgres c $ do
    blk <- runSelectReturningOne $ lookup_ (blocks database) (BlockId p)
    hum <- runSelectReturningOne $ lookup_ (headers database) (HeaderId p)
    pure (void blk <|> void hum)
  maybe (atomically $ writeTBQueue toLook b) mempty m

-- | Lookup a parent, write it to the Work Queue, and ready /its/ parent for analysis.
--
-- This can be ran over multiple threads.
looking :: Env -> TBQueue (T2 ChainId Parent) -> TBQueue (T2 ChainId Parent) -> IO ()
looking e@(Env _ c _ _) toCheck toLook = forever $ do
  T2 cid p@(Parent h) <- atomically $ readTBQueue toLook
  parent e p cid >>= \case
    Nothing -> T.putStrLn $ "[FAIL] Couldn't fetch parent: " <> unDbHash h
    Just hd -> do
      -- TODO Conflict handling?
      runBeamPostgres c . runInsert . insert (headers database) $ insertValues [hd]
      printf "[OKAY] Chain %d: %d: %s\n"
        (_header_chainId hd)
        (_header_height hd)
        (unDbHash $ _header_hash hd)
      let !next = T2 (ChainId $ _header_chainId hd) (Parent $ _header_parent hd)
      atomically $ writeTBQueue toCheck next

--------------------------------------------------------------------------------
-- Endpoints

-- | Fetch a parent header.
parent :: Env -> Parent -> ChainId -> IO (Maybe Header)
parent (Env m _ (Url u) (ChainwebVersion v)) (Parent (DbHash hsh)) (ChainId cid) = do
  req <- parseRequest url
  res <- httpLbs (req { requestHeaders = requestHeaders req <> octet }) m
  pure . hush . fmap (asHeader . asPow) . runGetLazy decodeBlockHeader $ responseBody res
  where
    url = "https://" <> u <> T.unpack query
    query = "/chainweb/0.0/" <> v <> "/chain/" <> T.pack (show cid) <> "/header/" <> hsh
    octet = [("accept", "application/octet-stream")]
