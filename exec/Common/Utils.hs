module Common.Utils where

import           Data.Text (Text)
import qualified Data.Text as T

tshow :: Show a => a -> Text
tshow = T.pack . show

hush :: Either e a -> Maybe a
hush (Left _)  = Nothing
hush (Right a) = Just a

note :: e -> Maybe a -> Either e a
note e Nothing  = Left e
note _ (Just a) = Right a

class Humanizable a where
  humanize :: a -> Text
