{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Chainweb.Data.Test.Parser
( tests
) where

------------------------------------------------------------------------------
import           Data.Aeson
import           Data.Text (Text)
import           Data.Text.Encoding
import           NeatInterpolation
import           Test.Tasty
import           Test.Tasty.HUnit
------------------------------------------------------------------------------
import           Chainweb.Api.BlockHeader
import           Chainweb.Api.BytesLE
import           Chainweb.Api.MinerData
import           ChainwebData.Types
import           ChainwebDb.Types.Block
------------------------------------------------------------------------------


tests :: TestTree
tests = testGroup "Block Parsing tests" [ parseUnitTests ]


parseUnitTests :: TestTree
parseUnitTests = testGroup "Parser unit tests"
    [ testCase "Block target parses correctly" $
      Right 14474011154664524427946373126085988481658748083205070504932198000989141204991
      @=? (getTarget <$> eitherDecodeStrict (encodeUtf8 blockJson))
    ]
  where
    blockJson :: Text
    blockJson =
      [text|
          {
            "creationTime":1572404750770821,
            "parent":"tn-ntdNtsYYcnBfoh819vJL9ldz-qkIXf4MDO6Kd-fw",
            "height":25,
            "hash":"N50q51qAGwKqugD4rK5fyvDQI5-9X-t3lK3c9V8WJI8",
            "chainId":0,
            "weight":"JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "featureFlags":0,
            "epochStart":1572404743031832,
            "adjacents": {
              "2":"Bm80lWv8r5nwpzp86TGE7oYmhJ4wg4jr58KJV0JqhLk",
              "5":"Vbogma-GMmPtrxF86kEzn8Zw4jkLxeXStekPprJXMLU",
              "3":"MsqHenmTRs42OVDB28oP_5MZCm8RaqDSVDrsVsTo2X0"
            },
            "payloadHash":"G5olU5tYfqU7FYXw99vyqEnURagZB-f2pL9ux7gwYks",
            "chainwebVersion":"mainnet01",
            "target":"_________________________________________x8",
            "nonce":"15652723681697760986"
          }
        |]

-- ------------------------------------------------------------------------ --
-- Test data

blockMinerData :: MinerData
blockMinerData = MinerData a "keys-all" [a]
  where
    a = "e7f7634e925541f368b827ad5c72421905100f6205285a78c19d7b4a38711805"

getTarget :: BlockHeader -> HashAsNum
getTarget h = _block_target $ asBlock (PowHeader h "aoeu") blockMinerData
