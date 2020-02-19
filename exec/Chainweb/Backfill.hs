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
import           Control.Concurrent.Async hiding (replicateConcurrently_)
import           Control.Concurrent.STM.TBQueue
import           Control.Error.Util (hush)
import           Control.Scheduler hiding (traverse_)
import qualified Data.Pool as P
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
backfill e@(Env _ c _ _) = withPool c $ \pool -> do
  bs <- P.withResource pool $ \conn -> runBeamPostgres conn . runSelectReturningList
    . select
    . orderBy_ (asc_ . _block_height)
    $ all_ (blocks database)
  q <- newTBQueueIO 1000
  caps <- getNumCapabilities
  concurrently_ (traverse_ (filling pool q) bs)
    $ replicateConcurrently_ Par' caps (work e pool q)

work :: Env -> P.Pool Connection -> TBQueue (T2 ChainId Parent) -> IO ()
work e pool q = forever $ do
  b <- atomically $ readTBQueue q
  looking e pool b >>= traverse_ (atomically . writeTBQueue q)

filling :: P.Pool Connection -> TBQueue (T2 ChainId Parent) -> Block -> IO ()
filling pool q b = checking pool pair >>= traverse_ (atomically . writeTBQueue q)
  where
    pair = T2 (ChainId $ _block_chainId b) (Parent $ _block_parent b)

checking :: P.Pool Connection -> T2 ChainId Parent -> IO (Maybe (T2 ChainId Parent))
checking pool b@(T2 _ (Parent p)) = P.withResource pool $ \c -> do
  m <- runBeamPostgres c $ do
    blk <- runSelectReturningOne $ lookup_ (blocks database) (BlockId p)
    hum <- runSelectReturningOne $ lookup_ (headers database) (HeaderId p)
    pure (void blk <|> void hum)
  pure $ maybe (Just b) (const Nothing) m

looking :: Env -> P.Pool Connection -> T2 ChainId Parent -> IO (Maybe (T2 ChainId Parent))
looking e pool (T2 cid p@(Parent h)) = parent e p cid >>= \case
  Nothing -> T.putStrLn ("[FAIL] Couldn't fetch parent: " <> unDbHash h) $> Nothing
  Just hd -> do
    P.withResource pool $ \c ->
      runBeamPostgres c . runInsert . insert (headers database) $ insertValues [hd]
    printf "[OKAY] Chain %d: %d: %s\n"
      (_header_chainId hd)
      (_header_height hd)
      (unDbHash $ _header_hash hd)
    pure . Just $ T2 (ChainId $ _header_chainId hd) (Parent $ _header_parent hd)

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
