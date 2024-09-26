module Main
( main
) where


import Chainweb.Data.Test.Parser as Parser
import Chainweb.Data.Test.Backfill as Backfill
import Chainweb.Data.Test.Verifier as Verifier
-- import Chainweb.Data.Test.Utils

import Test.Tasty
import Test.Tasty.Options

import Data.Proxy
import Debug.Trace

main :: IO ()
main = defaultMain $ testGroup "Chainweb Data Test suite"
    [ Parser.tests
    , Backfill.tests
    , Verifier.tests
    ]
