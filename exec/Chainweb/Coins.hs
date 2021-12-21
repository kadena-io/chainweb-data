{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Chainweb.Coins where

------------------------------------------------------------------------------
import           Control.Error
import           Control.Lens
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as CSV
import           Data.Decimal
import           Data.FileEmbed
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Time
import           Data.Text.Encoding
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Word
import           GHC.Generics
import           Text.Read
------------------------------------------------------------------------------

-- | Read in the reward csv via TH for deployment purposes.
--
rawMinerRewards :: ByteString
rawMinerRewards = $(embedFile "data/miner_rewards.csv")
{-# NOINLINE rawMinerRewards #-}

rawAllocations :: ByteString
rawAllocations = $(embedFile "data/token_payments.csv")

newtype CsvDecimal = CsvDecimal { _csvDecimal :: Decimal }
    deriving newtype (Eq, Ord, Show, Read)

instance CSV.FromField CsvDecimal where
    parseField f = either fail pure ev
      where
        ev = readEither =<< bimap show T.unpack (decodeUtf8' f)

newtype CsvTime = CsvTime { _csvTime :: UTCTime }
    deriving newtype (Eq, Ord, Show, Read)

instance CSV.FromField CsvTime where
    parseField f = either fail pure ev
      where
        ev = maybe (Left $ "Error parsing CsvTime " <> show f) (Right . CsvTime) .
          parseTimeM True defaultTimeLocale "%FT%TZ" =<<
          bimap show T.unpack (decodeUtf8' f)

newtype MinerRewards = MinerRewards
    { _minerRewards :: Map Word64 Decimal
      -- ^ The map of blockheight thresholds to miner rewards
    } deriving newtype (Eq, Ord, Show)


-- | Rewards table mapping 3-month periods to their rewards
-- according to the calculated exponential decay over 120 year period
--
minerRewardMap :: MinerRewards
minerRewardMap =
    case CSV.decode CSV.NoHeader (BL.fromStrict rawMinerRewards) of
      Left e -> error
        $ "cannot construct miner reward map: "
        <> show e
      Right vs -> MinerRewards $ M.fromList . V.toList . V.map formatRow $ vs
  where
    formatRow :: (Word64, CsvDecimal) -> (Word64, Decimal)
    formatRow (!a,!b) = (a, _csvDecimal b)

data AllocationEntry = AllocationEntry
    { _allocationName :: Text
    , _allocationTime :: CsvTime
    , _allocationKeysetName :: Text
    , _allocationAmount :: CsvDecimal
    , _allocationChain :: Text
    } deriving (Eq, Ord, Show, Generic)

instance CSV.FromRecord AllocationEntry

decodeAllocations
  :: ByteString
  -> Vector AllocationEntry
decodeAllocations bs =
  case CSV.decode CSV.HasHeader (BL.fromStrict bs) of
    Left e -> error
      $ "cannot construct genesis allocations: "
      <> show e
    Right as -> as

getCirculatingCoins :: Word64 -> UTCTime -> Decimal
getCirculatingCoins blockHeight blockTime =
  getTotalMiningRewards blockHeight + getTotalAllocations blockTime

getTotalAllocations :: UTCTime -> Decimal
getTotalAllocations blockTime =
    maybe 0 snd $ M.lookupLE blockTime cumulativeAllocations

cumulativeAllocations :: Map UTCTime Decimal
cumulativeAllocations = M.fromList $ go 0 0
  where
    v = decodeAllocations rawAllocations
    go total ind = case v V.!? ind of
      Nothing -> []
      Just a ->
        let time = _csvTime $ _allocationTime a
            amount = _csvDecimal $ _allocationAmount a
            (sectionTotal, nextInd) = getSection time amount (ind+1)
            newTotal = total + sectionTotal
         in (time, newTotal) : go newTotal nextInd
    getSection time total ind = case v V.!? ind of
      Nothing -> (total, ind)
      Just a -> if time == _csvTime (_allocationTime a)
                  then getSection time (total + _csvDecimal (_allocationAmount a)) (ind+1)
                  else (total, ind)

getTotalMiningRewards :: Word64 -> Decimal
getTotalMiningRewards height =
    lastTotal + fromIntegral (height - lastHeight) * reward
  where
    (lastHeight, (lastTotal, reward)) =
      fromMaybe (error msg) $ M.lookupLE height cumulativeRewards
    msg = "Error in getCirculatingCoins.  This shouldn't happen!"

cumulativeRewards :: Map Word64 (Decimal, Decimal)
cumulativeRewards = M.fromList $ go 0 0 $ M.toList $ _minerRewards minerRewardMap
  where
    go lastHeight total [] = [(lastHeight, (total, 0))]
    go lastHeight total ((height,reward):rs) = (lastHeight, (total, reward)) : go height t2 rs
      where
        t2 = total + fromIntegral (height - lastHeight) * reward

-- Helper functions

genesisDate :: UTCTime
genesisDate = UTCTime (fromGregorian 2019 10 30) 0

dateToHeight :: UTCTime -> Word64
dateToHeight t = blockHeight
  where
    genesis = fromGregorian 2019 10 30
    diff = diffUTCTime t (UTCTime genesis 0)
    blockHeight = round $ diff / 30

heightToDate :: Word64 -> UTCTime
heightToDate height = addUTCTime (fromIntegral height * 30) genesisDate

getCirculatingCoinsByDate :: UTCTime -> Decimal
getCirculatingCoinsByDate t = getCirculatingCoins (dateToHeight t) t

everyMonth :: [UTCTime]
everyMonth = filter (> genesisDate) $ do
  y <- [2019..2050]
  m <- [1..12]
  return $ UTCTime (fromGregorian y m 1) 0

everyYear :: [UTCTime]
everyYear = filter (> genesisDate) $ do
  y <- [2020..2050]
  return $ UTCTime (fromGregorian y 1 1) 0
