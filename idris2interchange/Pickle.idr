||| enough of pickle to make the interchange work, no more...
|||
||| makes heavy reference to Lib/pickletools.py (from around Python 3.12)
module Pickle

import Bytes
import Logging

||| some untyped (or Python Any-typed) representation of the output
||| of executing a pickle
data PickleAST = MkPickleAST


record VMState where
  constructor MkVMState
  stack: ()
  memo: ()   -- map of integers to objects. This is to support circular references so I'm not sure what this really should look like. Maybe I won't need it at all for the kind of data structures that are happening with the interchange?

-- define this signature before the body because we are mutually
-- recursive (or I could use a mutual block? what's the difference?)
step : {n : Nat} -> ByteBlock n -> VMState -> IO VMState

step_PROTO : {n : Nat} -> ByteBlock n -> VMState -> IO VMState

step_PROTO {n = Z} bb state = do
  ?error_PROTO_ran_off_end
  pure state

step_PROTO {n = S n'} bb state = do
  log "Opcode: PROTO"
  -- read a uint1
  (proto_ver, bb') <- bb_uncons bb
  putStr "Pickle protocol version: "
  printLn proto_ver
  step {n = n'} bb' state 

step {n = Z} bb state = do
    log "ERROR: Pickle VM ran off end of pickle"
    ?error_pickle_ran_off_end

step {n = S m} bb state = do
    (opcode, bb') <- bb_uncons bb

    putStr "Opcode number: "
    printLn opcode

    case opcode of 
      128 => step_PROTO {n = m} bb' state
      _ => ?error_unknown_opcode

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
unpickle ((S n) ** bb) = do
  log "beginning unpickle"

  let init_vm_state = MkVMState () ()

  end_vm_state <- step bb init_vm_state
  log "done with unpickle"
  pure MkPickleAST

unpickle (Z ** bb) = do
  log "unpickle not defined on empty byte sequence"
  ?error_unpickle_Z

