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
import           Control.Concurrent.STM.TVar
import           Control.Error.Util (hush)
import           Control.Scheduler (Comp(..), traverseConcurrently_)
import           Data.Serialize (runGetLazy)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Database.Beam
import           Database.Beam.Sqlite
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
  bs <- runBeamSqlite c . runSelectReturningList . select . all_ $ blocks database
  cn <- newTVarIO 0
  traverseConcurrently_ Par' (f cn) bs
  where
    f :: TVar Int -> Block -> IO ()
    f cn b = work e cn (Parent $ _block_parent b) (ChainId $ _block_chainId b)

-- | If necessary, fetch a parent Header and write it to the work queue.
work :: Env -> TVar Int -> Parent -> ChainId -> IO ()
work e@(Env _ c _ _) cn p@(Parent h) cid = inBlocks >>= \case
  Just _ -> pure ()
  Nothing -> inQueue >>= \case
    Just _ -> pure ()
    Nothing -> parent e p cid >>= \case
      Nothing -> T.putStrLn $ "[FAIL] Couldn't fetch parent: " <> unDbHash h
      Just hd -> do
        runBeamSqlite c . runInsert . insert (headers database) $ insertValues [hd]
        count <- atomically $ modifyTVar' cn (+ 1) >> readTVar cn
        T.putStrLn $ "[OKAY] Queued new parent: " <> unDbHash h <> " " <> T.pack (show count)
        work e cn (Parent $ _header_parent hd) cid
  where
    inBlocks = runBeamSqlite c . runSelectReturningOne $ lookup_ (blocks database) (BlockId h)
    inQueue = runBeamSqlite c . runSelectReturningOne $ lookup_ (headers database) (HeaderId h)

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
