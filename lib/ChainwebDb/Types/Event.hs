{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module ChainwebDb.Types.Event where

------------------------------------------------------------------------------
import BasePrelude
import Data.Aeson
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Database.Beam
import Database.Beam.Postgres (PgJSONB)
------------------------------------------------------------------------------
import ChainwebDb.Types.Block
------------------------------------------------------------------------------


------------------------------------------------------------------------------
data EventT f = Event
  { _ev_chainId :: C f Int64
  , _ev_block :: PrimaryKey BlockT f
  , _ev_creationTime :: C f UTCTime
  , _ev_requestKey :: C f Text
  , _ev_txid :: C f (Maybe Int64)
  , _ev_idx :: C f Int64
  , _ev_name :: C f Text
  , _ev_module :: C f Text
  , _ev_moduleHash :: C f Text
  , _ev_param_1 :: C f (PgJSONB Value)
  , _ev_param_2 :: C f (PgJSONB Value)
  , _ev_param_3 :: C f (PgJSONB Value)
  , _ev_params :: C f (PgJSONB [Value])
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Event
  (LensFor ev_chainId)
  (BlockId (LensFor ev_block))
  (LensFor ev_creationTime)
  (LensFor ev_requestKey)
  (LensFor ev_txid)
  (LensFor ev_idx)
  (LensFor ev_name)
  (LensFor ev_module)
  (LensFor ev_moduleHash)
  (LensFor ev_param_1)
  (LensFor ev_param_2)
  (LensFor ev_param_3)
  (LensFor ev_params)
  = tableLenses

type Event = EventT Identity
type EventId = PrimaryKey EventT Identity

-- deriving instance Eq (PrimaryKey TransactionT Identity)
-- deriving instance Eq (PrimaryKey TransactionT Maybe)
-- deriving instance Eq Transaction
-- deriving instance Show (PrimaryKey TransactionT Identity)
-- deriving instance Show (PrimaryKey TransactionT Maybe)
-- deriving instance Show Transaction
-- deriving instance Show (TransactionT Maybe)
-- deriving instance Ord (PrimaryKey TransactionT Identity)
-- deriving instance Ord (PrimaryKey TransactionT Maybe)

instance Table EventT where
  data PrimaryKey EventT f = EventId (C f Text) (C f Int64)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey t = EventId (_ev_requestKey t) (_ev_idx t)
