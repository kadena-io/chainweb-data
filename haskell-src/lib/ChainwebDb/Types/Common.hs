{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module ChainwebDb.Types.Common where

------------------------------------------------------------------------------
import           Data.Text (Text)
import qualified Data.Text as T
import           Database.Beam
import           Database.Beam.Backend.SQL hiding (tableName)
import           Database.Beam.Backend.SQL.Row ()
import           Database.Beam.Backend.SQL.SQL92 ()
import           Database.Beam.Backend.Types
import           Database.Beam.Migrate
------------------------------------------------------------------------------
import           ChainwebDb.Types.DbHash
------------------------------------------------------------------------------

data ReqKeyOrCoinbase = RKCB_RequestKey (DbHash TxHash) | RKCB_Coinbase
  deriving (Eq, Ord, Generic)

instance Show ReqKeyOrCoinbase where
  show RKCB_Coinbase = "cb"
  show (RKCB_RequestKey rk) = T.unpack $ unDbHash rk

getTxHash :: ReqKeyOrCoinbase -> T.Text
getTxHash = \case
      RKCB_RequestKey txhash -> unDbHash txhash
      RKCB_Coinbase -> "<coinbase>"

rkcbFromText :: Text -> ReqKeyOrCoinbase
rkcbFromText "cb" = RKCB_Coinbase
rkcbFromText rk = RKCB_RequestKey $ DbHash rk

instance BeamMigrateSqlBackend be => HasSqlEqualityCheck be ReqKeyOrCoinbase

instance BeamMigrateSqlBackend be => HasDefaultSqlDataType be ReqKeyOrCoinbase where
  defaultSqlDataType _ _ _ = varCharType Nothing Nothing

instance HasSqlValueSyntax be String => HasSqlValueSyntax be ReqKeyOrCoinbase where
  sqlValueSyntax = autoSqlValueSyntax

instance (BeamBackend be, FromBackendRow be Text) => FromBackendRow be ReqKeyOrCoinbase where
  fromBackendRow = rkcbFromText <$> fromBackendRow
