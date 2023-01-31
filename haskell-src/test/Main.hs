module Main
( main
) where


import Chainweb.Data.Test.Parser as Parser
import Chainweb.Data.Test.Backfill as Backfill

import Test.Tasty


main :: IO ()
main = defaultMain $ testGroup "Chainweb Data Test suite"
    [ Parser.tests
    , Backfill.tests
    ]
