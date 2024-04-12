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

module ChainwebDb.Types.Verifier where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Int
import           Data.Text (Text)
import           Database.Beam
import           Database.Beam.Backend.SQL.Row ()
import           Database.Beam.Backend.SQL.SQL92 ()
import           Database.Beam.Postgres
------------------------------------------------------------------------------
import           ChainwebDb.Types.DbHash
------------------------------------------------------------------------------


data VerifierT f = Verifier
  { _verifier_requestkey :: C f (DbHash TxHash)
  , _verifier_idx :: C f Int32
  , _verifier_name :: C f (Maybe Text)
  , _verifier_proof :: C f (Maybe Text)
  , _verifier_caps :: C f (PgJSONB [Value])
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type Verifier = VerifierT Identity
type VerifierId = PrimaryKey VerifierT Identity

instance Table VerifierT where
  data PrimaryKey VerifierT f = VerifierT (C f (DbHash TxHash)) (C f Int32)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = VerifierT <$> _verifier_requestkey <*> _verifier_idx
