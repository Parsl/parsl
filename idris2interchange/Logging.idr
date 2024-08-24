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
log : HasErr AppHasIO es => String -> App es ()
log msg = primIO $ do
  putStr " * "
  now <- clockTime UTC
  print now
  putStr " "
  putStrLn msg


covering public export
logv : HasErr AppHasIO es => Show s => String -> s -> App es ()
logv msg v = primIO $ do
  putStr " * "
  now <- clockTime UTC
  print now
  putStr " "
  putStr msg
  putStr ": "
  printLn v
