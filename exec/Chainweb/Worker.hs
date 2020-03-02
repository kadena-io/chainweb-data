module Chainweb.Worker ( writes ) where

import           BasePrelude hiding (delete, insert)
import           Chainweb.Database
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.MinerKey
import           ChainwebDb.Types.Transaction
import qualified Data.Pool as P
import qualified Data.Text as T
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL.BeamExtensions
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Full (insert, onConflict)

---

-- | Write a Block and its Transactions to the database. Also writes the Miner
-- if it hasn't already been via some other block.
writes :: P.Pool Connection -> Block -> [T.Text] -> [Transaction] -> IO ()
writes pool b ks ts = P.withResource pool $ \c -> runBeamPostgres c $ do
  -- Write Pub Key many-to-many relationships if unique --
  runInsert
    $ insert (minerkeys database) (insertValues $ map (MinerKey (pk b)) ks)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the Block if unique --
  runInsert
    $ insert (blocks database) (insertValues [b])
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- Write the TXs if unique --
  runInsert
    $ insert (transactions database) (insertValues ts)
    $ onConflict (conflictingFields primaryKey) onConflictDoNothing
  -- liftIO $ printf "[OKAY] Chain %d: %d: %s %s\n"
  --   (_block_chainId b)
  --   (_block_height b)
  --   (unDbHash $ _block_hash b)
  --   (map (const '.') ts)
