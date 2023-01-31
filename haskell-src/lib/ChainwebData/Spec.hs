{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module ChainwebData.Spec where


import Control.Lens
import ChainwebData.Api

import Data.Proxy

import Data.OpenApi.ParamSchema
import Data.OpenApi.Schema
import Servant.OpenApi
import ChainwebData.Pagination
import Chainweb.Api.ChainId
import ChainwebData.TxSummary
import Data.OpenApi

import ChainwebData.EventDetail (EventDetail)
import ChainwebData.Util
import qualified Data.Aeson as A
import ChainwebData.TxDetail
import ChainwebData.TransferDetail (TransferDetail)
import Chainweb.Api.StringEncoded (StringEncoded)
import Data.Scientific (Scientific)

instance ToSchema A.Value where
  declareNamedSchema _ = pure $ NamedSchema (Just "AnyValue") mempty

deriving newtype instance ToParamSchema Limit
deriving newtype instance ToParamSchema Offset
deriving newtype instance ToParamSchema EventParam
deriving newtype instance ToParamSchema EventName
deriving newtype instance ToParamSchema EventModuleName
deriving newtype instance ToParamSchema RequestKey
deriving newtype instance ToParamSchema ChainId
deriving newtype instance ToParamSchema NextToken

instance ToSchema TxSummary where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = drop 11 }

deriving anyclass instance ToSchema TxResult

instance ToSchema EventDetail where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = lensyConstructorToNiceJson 10 }

instance ToSchema TxDetail where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = lensyConstructorToNiceJson 10 }

instance ToSchema TxEvent where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = lensyConstructorToNiceJson 9 }

instance ToSchema TransferDetail where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = lensyConstructorToNiceJson 10 }

instance ToSchema ChainwebDataStats where
  declareNamedSchema = genericDeclareNamedSchema
    defaultSchemaOptions{ fieldLabelModifier = drop 5 }

instance ToSchema (StringEncoded Scientific) where
  declareNamedSchema _ = pure $ NamedSchema (Just "StringEncodedNumber") $ mempty
    & type_ ?~ OpenApiString
    & example ?~ A.String "-1234.5e6"
    & pattern ?~ "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"

spec :: OpenApi
spec = toOpenApi (Proxy :: Proxy ChainwebDataApi)

