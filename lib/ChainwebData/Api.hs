{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeOperators #-}

module ChainwebData.Api where

------------------------------------------------------------------------------
import           Data.Proxy
import           Servant.API
------------------------------------------------------------------------------
import           ChainwebData.Pagination
import           ChainwebData.TxSummary
------------------------------------------------------------------------------


type ChainwebDataApi = "txs" :> TxApi

type TxApi = RecentTxsApi :<|> TxSearchApi

type RecentTxsApi = "recent" :> Get '[JSON] [TxSummary]
type TxSearchApi = "search" :> LimitParam :> OffsetParam :> SearchParam :> Get '[JSON] [TxSummary]

chainwebDataApi :: Proxy ChainwebDataApi
chainwebDataApi = Proxy
