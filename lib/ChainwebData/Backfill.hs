module ChainwebData.Backfill
( lookupPlan
, oldLookupPlan
) where


import Control.Arrow ((&&&))

import Data.Bifunctor
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe)

import Chainweb.Api.ChainId (ChainId(..))
import ChainwebData.Genesis
import ChainwebData.Types


lookupPlan :: Map ChainId Int -> [(ChainId, Low, High)]
lookupPlan = M.foldrWithKey go []
  where
    go cid cmin acc
       | genesisHeight cid > cmin = acc
       | otherwise =
         let
            cmax = max genesis $ cmin - 1
            -- calculate genesis height for chain id
            --
            genesis = genesisHeight cid

            -- calculate 100-entry batches using min blockheight @cmin@
            -- and genesis height
            --
            ranges = map (Low . last &&& High . head) $
              groupsOf 100 [cmax, cmax - 1 .. genesis]

            -- calculate high water entry against minimum block height for cid
            --
            high = High cmax

            -- given a lo-hi window (calculated in 'ranges'), choose best window
            -- against the 'chigh' water mark and calcaulated ranges
            --
            window (low@(Low l), high') lst
              | high' > high, l <= cmax, l >= genesis =
                (cid, low, high):lst
              | high' <= high, l <= cmax, l >= genesis =
                (cid, low, high'):lst
              | otherwise = lst

        in foldr window acc ranges

-- | Based on some initial minimum heights per chain, form a lazy list of block
-- ranges that need to be looked up.
--
-- _/N.B./_: this is the old code for lookup plans.
--
oldLookupPlan :: Map ChainId Int -> [(ChainId, Low, High)]
oldLookupPlan mins = concatMap (\pair -> mapMaybe (g pair) asList) ranges
  where
    maxi :: Int
    maxi = max 0 $ maximum (M.elems mins) - 1

    asList :: [(ChainId, High)]
    asList = map (second (\n -> High . max 0 $ n - 1)) $ M.toList mins

    ranges :: [(Low, High)]
    ranges = map (Low . last &&& High . head) $ groupsOf 100 [maxi, maxi-1 .. 0]

    g :: (Low, High) -> (ChainId, High) -> Maybe (ChainId, Low, High)
    g (l@(Low l'), u) (cid, mx@(High mx'))
      | u > mx && l' <= mx' = Just (cid, l, mx)
      | u <= mx = Just (cid, l, u)
      | otherwise = Nothing
