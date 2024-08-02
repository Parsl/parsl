||| enough of pickle to make the interchange work, no more...
module Pickle

import Bytes
import Logging

||| some untyped (or Python Any-typed) representation of the output
||| of executing a pickle
data PickleAST = MkPickleAST

||| Takes some representation of a byte sequence containing a pickle and
||| executes that pickle program, leaving something more like an AST of
||| the final object. That AST can be turned into an idris2 value by some
||| later function.
||| TODO: I'd like to be able to log pickle decoding without being embedded
||| in IO, so some effects system stuff? That maybe requires ByteBlock to be
||| immutable (which it isn't because ZMQ ops can edit it?) but maybe its
||| then interesting to flag ZMQ messages as frozen so they can't be
||| used with mutating ZMQ operations? Then are they safe to be used
||| functionally? maybe need some linear stuff on usage of msg object
||| to let that happen, session style?
export
unpickle : (n: Nat ** (ByteBlock n)) -> IO PickleAST
unpickle i = do
  log "beginning unpickle"
  pure MkPickleAST
