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
import Database.Beam
import Database.Beam.Postgres (PgJSONB)
------------------------------------------------------------------------------
import ChainwebDb.Types.DbHash
------------------------------------------------------------------------------
data EventT f = Event
  { _ev_requestkey :: C f (Maybe (DbHash PayloadHash))
  , _ev_block :: C f (Maybe (DbHash BlockHash))
  , _ev_chainid :: C f Int64
  , _ev_height :: C f Int64
  , _ev_idx :: C f Int64
  , _ev_qualName :: C f Text
  , _ev_name :: C f Text
  , _ev_module :: C f Text
  , _ev_moduleHash :: C f Text
  , _ev_paramText :: C f Text
  , _ev_params :: C f (PgJSONB [Value])
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

Event
  (LensFor ev_requestkey)
  (LensFor ev_block)
  (LensFor ev_chainid)
  (LensFor ev_height)
  (LensFor ev_idx)
  (LensFor ev_name)
  (LensFor ev_qualName)
  (LensFor ev_module)
  (LensFor ev_moduleHash)
  (LensFor ev_paramText)
  (LensFor ev_params)
  = tableLenses

type Event = EventT Identity
type EventId = PrimaryKey EventT Identity

instance Table EventT where
  data PrimaryKey EventT f = EventId (C f Int64) (C f Int64) (C f Int64)
    deriving stock (Generic)
    deriving anyclass (Beamable)
{-
In the development of this table's schema, we had considered not attaching a
primary key. Unfortunately, even in beam version 0.9.1.0, this ORM still seems
to demand that any tables declared in Haskell have their own respective primary
keys. If this is not included, migrations may fail with cryptic errors. Such as
below:

chainweb-data: internal error: Unable to commit 1048576 bytes of memory
    (GHC version 8.6.5 for x86_64_unknown_linux)
    Please report this as a GHC bug:  http://www.haskell.org/ghc/reportabug
Killed
-}
  primaryKey = EventId <$> _ev_chainid <*> _ev_height <*> _ev_idx
