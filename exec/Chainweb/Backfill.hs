{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Chainweb.Backfill ( backfill ) where

import           BasePrelude hiding (insert, range)
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.ChainId (ChainId(..))
import           Chainweb.Api.Hash
import           Chainweb.Database
import           Chainweb.Env
import           Chainweb.Types (asBlock, asPow, groupsOf, hash)
import           Chainweb.Worker
import           ChainwebDb.Types.Block
import           Control.Scheduler hiding (traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import qualified Data.Pool as P
import qualified Data.Text as T
import           Data.Tuple.Strict (T2(..))
import           Data.Witherable.Class (wither)
import           Database.Beam
import           Database.Beam.Postgres (Connection, runBeamPostgres)
import           Lens.Micro ((^..))
import           Lens.Micro.Aeson (key, values, _JSON)
import           Network.HTTP.Client hiding (Proxy)

---

backfill :: Env -> IO ()
backfill e@(Env _ c _ _) = withPool c $ \pool -> do
  mins <- minHeights pool
  print mins
  traverseConcurrently_ Par' (f pool) $ lookupPlan mins
  where
    f :: P.Pool Connection -> (ChainId, Low, High) -> IO ()
    f pool range = headersBetween e range >>= \case
      [] -> printf "[FAIL] headersBetween: %s" $ show range
      hs -> traverse_ (g pool) hs

    g :: P.Pool Connection -> BlockHeader -> IO ()
    g pool bh = do
      let !pair = T2 (_blockHeader_chainId bh) (hash $ _blockHeader_payloadHash bh)
      payload e pair >>= \case
        Nothing -> printf "[FAIL] Couldn't fetch parent for: "
          (hashB64U $ _blockHeader_hash bh)
        Just pl -> do
          let !m = miner pl
              !b = asBlock (asPow bh) m
              !t = txs b pl
          writes pool b $ T2 m t

newtype Low = Low Int deriving newtype (Show)
newtype High = High Int deriving newtype (Eq, Ord, Show)

-- | For all blocks written to the DB, find the shortest (in terms of block
-- height) for each chain.
minHeights :: P.Pool Connection -> IO (Map ChainId Int)
minHeights pool = M.fromList <$> wither (\cid -> fmap (cid,) <$> f cid) chains
  where
    chains :: [ChainId]
    chains = map ChainId [0..9]  -- TODO Make configurable.

    -- | Get the current minimum height of any block on some chain.
    f :: ChainId -> IO (Maybe Int)
    f (ChainId cid) = fmap (fmap _block_height)
      $ P.withResource pool
      $ \c -> runBeamPostgres c
      $ runSelectReturningOne
      $ select
      $ limit_ 1
      $ orderBy_ (asc_ . _block_height)
      $ filter_ (\b -> _block_chainId b ==. val_ cid)
      $ all_ (blocks database)

-- | Based on some initial minimum heights per chain, form a lazy list of block
-- ranges that need to be looked up.
lookupPlan :: Map ChainId Int -> [(ChainId, Low, High)]
lookupPlan mins = concatMap (\pair -> mapMaybe (g pair) asList) ranges
  where
    maxi :: Int
    maxi = max 0 $ maximum (M.elems mins) - 1

    asList :: [(ChainId, High)]
    asList = map (second (\n -> High . max 0 $ n - 1)) $ M.toList mins

    ranges :: [(Low, High)]
    ranges = map (Low . last &&& High . head) $ groupsOf 100 [maxi .. 0]

    g :: (Low, High) -> (ChainId, High) -> Maybe (ChainId, Low, High)
    g (l@(Low l'), u) (cid, mx@(High mx'))
      | u > mx && l' <= mx' = Just (cid, l, mx)
      | u <= mx = Just (cid, l, u)
      | otherwise = Nothing

--------------------------------------------------------------------------------
-- Endpoints

headersBetween :: Env -> (ChainId, Low, High) -> IO [BlockHeader]
headersBetween (Env m _ (Url u) (ChainwebVersion v)) (cid, Low low, High up) = do
  req <- parseRequest url
  res <- httpLbs (req { requestHeaders = requestHeaders req <> encoding }) m
  pure . (^.. key "items" . values . _JSON) $ responseBody res
  where
    url = "https://" <> u <> query
    query = printf "/chainweb/0.0/%s/chain/%d/header?minheight=%d&maxheight=%d"
      (T.unpack v) (unChainId cid) low up
    encoding = [("accept", "application/json;blockheader-encoding=object")]

-- TODO Use the binary encodings instead?

-- | Fetch a parent header.
-- parent :: Env -> Parent -> ChainId -> IO (Maybe Header)
-- parent (Env m _ (Url u) (ChainwebVersion v)) (Parent (DbHash hsh)) (ChainId cid) = do
--   req <- parseRequest url
--   res <- httpLbs (req { requestHeaders = requestHeaders req <> octet }) m
--   pure . hush . fmap (asHeader . asPow) . runGetLazy decodeBlockHeader $ responseBody res
--   where
--     url = "https://" <> u <> T.unpack query
--     query = "/chainweb/0.0/" <> v <> "/chain/" <> T.pack (show cid) <> "/header/" <> hsh
--     octet = [("accept", "application/octet-stream")]
