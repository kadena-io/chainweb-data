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

module ChainwebDb.Types.Signer where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Int
import           Data.Text (Text)
import           Database.Beam
import           Database.Beam.Backend.SQL hiding (tableName)
import           Database.Beam.Backend.SQL.Row ()
import           Database.Beam.Backend.SQL.SQL92 ()
import           Database.Beam.Migrate
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Syntax (PgValueSyntax)
------------------------------------------------------------------------------
import           ChainwebDb.Types.DbHash
------------------------------------------------------------------------------

newtype Signature = Signature { unSignature :: Text }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
  deriving newtype (HasSqlValueSyntax PgValueSyntax)
  deriving newtype (FromBackendRow Postgres, HasSqlEqualityCheck Postgres)

instance BeamMigrateSqlBackend be => HasDefaultSqlDataType be Signature where
  defaultSqlDataType _ _ _ = varCharType Nothing Nothing

data SignerT f = Signer
  { _signer_requestkey :: C f (DbHash TxHash)
  , _signer_idx :: C f Int32
  , _signer_pubkey :: C f Text
  , _signer_scheme :: C f (Maybe Text)
  , _signer_addr :: C f (Maybe Text)
  , _signer_caps :: C f (PgJSONB [Value])
  , _signer_sig :: C f Signature
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

type Signer = SignerT Identity
type SignerId = PrimaryKey SignerT Identity

instance Table SignerT where
  data PrimaryKey SignerT f = SignerId (C f (DbHash TxHash)) (C f Int32)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = SignerId <$> _signer_requestkey <*> _signer_idx

