{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Chainweb.Server where

------------------------------------------------------------------------------
import           Control.Monad.Except
import qualified Data.Pool as P
import           Data.Text (Text)
import           Database.Beam hiding (insert)
import           Database.Beam.Backend.SQL
import           Database.Beam.Postgres
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Network.Wai.Middleware.Cors
import           Servant.API
import           Servant.Server
------------------------------------------------------------------------------
import           Chainweb.Database
import           Chainweb.Env
import           ChainwebData.Types ()
import           ChainwebData.Api
import           ChainwebData.Pagination
import           ChainwebData.TxSummary
import           ChainwebDb.Types.Block
import           ChainwebDb.Types.Transaction
------------------------------------------------------------------------------

setCors :: Middleware
setCors = cors . const . Just $ simpleCorsResourcePolicy
    { corsRequestHeaders = simpleHeaders
    }

apiServer :: Env -> IO ()
apiServer env = do
  Network.Wai.Handler.Warp.run 8080 $ setCors $ serve chainwebDataApi $ server env

server :: Env -> Server ChainwebDataApi
server env = recentTxs conn
        :<|> searchTxs conn
  where
    conn = _env_dbConnectInfo env

instance BeamSqlBackendIsString Postgres (Maybe Text)
instance BeamSqlBackendIsString Postgres (Maybe String)

searchTxs :: Connect -> Maybe Limit -> Maybe Offset -> Maybe Text -> Handler [TxSummary]
searchTxs _ _ _ Nothing = do
    throwError $ err404 { errBody = "You must specify a search string" }
searchTxs dbConnInfo limit offset (Just search) = do
    liftIO $ withPool dbConnInfo $ \pool -> P.withResource pool $ \c -> do
      res <- runBeamPostgres c $
        runSelectReturningList $ select $ do
        orderBy_ (desc_ . getHeight) $ limit_ lim $ offset_ off $ do
          tx <- all_ (_cddb_transactions database)
          blk <- all_ (_cddb_blocks database)
          guard_ (_tx_block tx `references_` blk)
          guard_ (_tx_code tx `like_` val_ (Just searchString))
          return
             ( (_tx_chainId tx)
             , (_block_height blk)
             , (unBlockId $ _tx_block tx)
             , (_tx_creationTime tx)
             , (_tx_requestKey tx)
             , (_tx_sender tx)
             , (_tx_code tx)
             , (_tx_goodResult tx)
             )
      return $ mkSummary <$> res
  where
    lim = maybe 10 (min 100 . unLimit) limit
    off = maybe 0 unOffset offset
    getHeight (_,a,_,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,g,h) = TxSummary a b c d e f g (maybe TxFailed (const TxSucceeded) h)
    searchString = "%" <> search <> "%"

data h :. t = h :. t deriving (Eq,Ord,Show,Read,Typeable)
infixr 3 :.

type instance QExprToIdentity (a :. b) = (QExprToIdentity a) :. (QExprToIdentity b)
type instance QExprToField (a :. b) = (QExprToField a) :. (QExprToField b)


recentTxs :: Connect -> Handler [TxSummary]
recentTxs dbConnInfo = do
    liftIO $ withPool dbConnInfo $ \pool -> P.withResource pool $ \c -> do
      res <- runBeamPostgres c $
        runSelectReturningList $ select $ do
        orderBy_ (desc_ . getHeight) $ limit_ 20 $ do
          tx <- all_ (_cddb_transactions database)
          blk <- all_ (_cddb_blocks database)
          guard_ (_tx_block tx `references_` blk)
          return
             ( (_tx_chainId tx)
             , (_block_height blk)
             , (unBlockId $ _tx_block tx)
             , (_tx_creationTime tx)
             , (_tx_requestKey tx)
             , (_tx_sender tx)
             , (_tx_code tx)
             , (_tx_goodResult tx)
             )
      return $ mkSummary <$> res
  where
    getHeight (_,a,_,_,_,_,_,_) = a
    mkSummary (a,b,c,d,e,f,g,h) = TxSummary a b c d e f g (maybe TxFailed (const TxSucceeded) h)
