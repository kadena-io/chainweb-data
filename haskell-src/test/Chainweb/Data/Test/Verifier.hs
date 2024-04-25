{-# language LambdaCase #-}
{-# language OverloadedStrings #-}
{-# language RecordWildCards #-}
{-# language TypeApplications #-}
module Chainweb.Data.Test.Verifier
( tests
) where

import Control.Exception
import Control.Lens
import Control.Monad
import qualified Data.Aeson as A
import           Data.Aeson.KeyMap (fromList)
import           Data.Aeson.Lens
import qualified Data.ByteString.Lazy as BL
import System.Directory

import Options.Applicative

import Chainweb.Api.Verifier
-- import Chainweb.Data.Test.Utils

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests =
  testGroup "Verifier plugin tests"
    [parseVerifier
    , parseVerifierFromCommandText ]


parseVerifier :: TestTree
parseVerifier = testCase "verifier decoding test" $ do
    rawFile <- BL.readFile "haskell-src/test/test-verifier.txt"
    either (throwIO . userError) (expectedValue @=?) $ A.eitherDecode @Verifier rawFile
  where
    expectedValue =
      Verifier
        {_verifier_name = Just "allow"
        , _verifier_proof = A.Object (fromList [("keysetref",A.Object (fromList [("ksn",A.String "\120167\&4hy3@un~\185384tYM|y_"),("ns",A.String "?k%B\96883\153643\38839\68129P\139946=\97190$Wk\95172es8QQVIu\197146ypX")]))])
        , _verifier_capList = []
        }

parseVerifierFromCommandText :: TestTree
parseVerifierFromCommandText = testCase "Command Text verifier decoding test" $ do
    rawFile <- BL.readFile "haskell-src/test/command-text-with-verifier.txt"
    either (throwIO . userError) (expectedValue @=?) $
      A.eitherDecode @A.Value rawFile >>= \r ->
           r ^? key "cmd" . _String . key "verifiers" . _JSON
           & note verifyMsg
  where
    verifyMsg = "Can't find expected verifiers key command text"
    note msg = maybe (Left msg) Right
    expectedValue =
      [Verifier
        {_verifier_name = Just "allow"
        , _verifier_proof = A.String "emmanuel"
        , _verifier_capList = []
        }]

-- TODO: Maybe come back to this later
-- findVerifiers :: FilePath -> TestTree
-- findVerifiers path =
--   withResource (mkOpts modernDefaultOptions) freeOpts $ \opts' ->
--     withResource (open opts' path) closeRocksDb test
--   where
--     open opts' path = do
--       Options'{..} <- opts'
--       openReadOnlyRocksDb path _optsPtr
--     test _iordb = testCase "inner" $ assertEqual "testing" 1 1

