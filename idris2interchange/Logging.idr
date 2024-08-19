module Logging

import System.Clock

%default total

||| Output a log message
|||
||| This is just a print right now...
||| What would it look like if I was trying to do macro-style eliding
||| of log calls entirely? something involving the elaborator?
public export
log : String -> IO ()
log msg = do
  putStr " * "
  now <- clockTime UTC
  print now
  putStr " "
  putStrLn msg


