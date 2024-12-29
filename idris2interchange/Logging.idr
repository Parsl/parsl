module Logging

import Control.App
import System.Clock

%default total


public export
record LogConfig where
  constructor MkLogConfig
  enabled : Bool


public export
loggingEnabled : State LogConfig LogConfig es => App {l} es Bool
loggingEnabled = do
  MkLogConfig v <- get LogConfig
  pure v


||| Output a log message
|||
||| This is just a print right now...
||| What would it look like if I was trying to do macro-style eliding
||| of log calls entirely? something involving the elaborator?
covering public export
log : (State LogConfig LogConfig es, HasErr AppHasIO es) => String -> App {l} es ()
log msg = do
  en <- loggingEnabled
  when en $ primIO $ do
    putStr " * "
    now <- clockTime UTC
    print now
    putStr " "
    putStrLn msg


covering public export
logv : (State LogConfig LogConfig es, HasErr AppHasIO es) => Show s => String -> Lazy s -> App {l} es ()
logv msg v = do
  en <- loggingEnabled
  when en $ primIO $ do
    putStr " * "
    now <- clockTime UTC
    print now
    putStr " "
    putStr msg
    putStr ": "
    printLn v


namespace Log.Linear

  covering public export
  log : (State LogConfig LogConfig es, HasErr AppHasIO es) => String -> App1 {u=Any} es ()
  log msg = app $ log msg

  covering public export
  logv : (State LogConfig LogConfig es, HasErr AppHasIO es) => Show s => String -> Lazy s -> App1 {u=Any} es ()
  logv msg v = app $ logv msg v
