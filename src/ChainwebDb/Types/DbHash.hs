{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

module ChainwebDb.Types.DbHash where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Text (Text)
import           Database.Beam
------------------------------------------------------------------------------


-- | DB hashes stored as Base64Url encoded text for more convenient querying.
newtype DbHash = DbHash { unDbHash :: Text }
  deriving (Eq,Ord,Show,Generic)

instance ToJSON DbHash where
    toEncoding = genericToEncoding defaultOptions

instance FromJSON DbHash
