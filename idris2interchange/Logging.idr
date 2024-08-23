module Logging

import Control.App
import System.Clock

%default total

||| Output a log message
|||
||| This is just a print right now...
||| What would it look like if I was trying to do macro-style eliding
||| of log calls entirely? something involving the elaborator?
covering public export
log : String -> App Init ()
log msg = primIO $ do
  putStr " * "
  now <- clockTime UTC
  print now
  putStr " "
  putStrLn msg


