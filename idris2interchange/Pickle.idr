||| enough of pickle to make the interchange work, no more...
|||
||| makes heavy reference to Lib/pickletools.py (from around Python 3.12)
module Pickle

import Control.App
import Data.Monoid.Exponentiation
import Generics.Derive

import Bytes
import Logging

%language ElabReflection

-- ISSUE: It should be possible for this to be total, on the length of the
-- block of bytes, but this doesn't work at time of writing: I guess
-- I haven't conveyed that the recursion terminates properly.
-- %default total

||| some untyped (or Python Any-typed) representation of the output
||| of executing a pickle.
||| PickleMark is a special thing here because its used on the stack
||| but not expected to be exposed as part of the final value.
|||
||| TODO: the Eq instance derived automatically here will have different
||| equality behaviour to Python, because it will be compared in order,
||| while Python equality is key-order independent.
public export
data PickleAST = PickleUnicodeString String
               | PickleInteger Integer
               | PickleFloat Double
               | PickleTuple (List PickleAST)
               | PickleList (List PickleAST)
               | PickleDict (List (PickleAST, PickleAST))
               | PickleMark
               | PickleBytes (List Bits8)
               | PickleNone
               | PickleGlobal PickleAST PickleAST  -- should always be a pair of strings, i think?
               | PickleObj PickleAST PickleAST  -- NEWOBJ creates a class object (eg PickleGlobal) and args to __new__
%runElab derive "PickleAST" [Generic, Meta, Show, Eq]

export
FromString PickleAST where
  fromString s = PickleUnicodeString s

record VMState where
  constructor MkVMState
  stack: List PickleAST
  memo: List PickleAST  -- sequential map of integers to objects - memo key n is at position n in the list so we don't need to store the key


-- read an 8-byte integer - uint8 is the pickletools ArgumentDescriptor name.
-- if there are not 8 bytes left in ByteBlock, explode...
-- TODO: this could look nicer with a parser monad/parser applicative
-- rather than threading the shrinking bb?
read_uint8 : HasErr AppHasIO es => (1 _ : ByteBlock) -> App1 es (Res Int (const ByteBlock))
read_uint8 bb = do
  (byte1 # bb1) <- bb_uncons bb 
  (byte2 # bb2) <- bb_uncons bb1 
  (byte3 # bb3) <- bb_uncons bb2
  (byte4 # bb4) <- bb_uncons bb3 
  (byte5 # bb5) <- bb_uncons bb4 
  (byte6 # bb6) <- bb_uncons bb5 
  (byte7 # bb7) <- bb_uncons bb6 
  (byte8 # bb8) <- bb_uncons bb7 

  -- bytes are bytes but we want an Int...

  let v = (((((((
              cast byte8) * 256 + 
              cast byte7) * 256 + 
              cast byte6) * 256 + 
              cast byte5) * 256 + 
              cast byte4) * 256 + 
              cast byte3) * 256 + 
              cast byte2) * 256 +
              cast byte1
  pure1 $ v # bb8


read_uint4 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 es (Res Int (const ByteBlock))
read_uint4 bb = do
  (byte1 # bb1) <- bb_uncons bb 
  (byte2 # bb2) <- bb_uncons bb1 
  (byte3 # bb3) <- bb_uncons bb2
  (byte4 # bb4) <- bb_uncons bb3 

  logv "byte1" byte1
  logv "byte2" byte2
  logv "byte3" byte3
  logv "byte4" byte4

  -- bytes are bytes but we want an Int...

  let v = (((
              cast byte4) * 256 + 
              cast byte3) * 256 + 
              cast byte2) * 256 +
              cast byte1

  logv "byte combined to v" v
  pure1 $ v # bb4


read_uint2 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 es (Res Int (const ByteBlock))
read_uint2 bb = do
  (byte1 # bb1) <- bb_uncons bb 
  (byte2 # bb2) <- bb_uncons bb1 

  logv "byte1" byte1
  logv "byte2" byte2

  -- bytes are bytes but we want an Int...

  let v = (
              cast byte2) * 256 +
              cast byte1

  logv "byte combined to v" v
  pure1 $ v # bb2


read_double8 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 es (Res Double (const ByteBlock))
read_double8 bb = do
  (byte1 # bb1) <- bb_uncons bb 
  (byte2 # bb2) <- bb_uncons bb1 
  (byte3 # bb3) <- bb_uncons bb2
  (byte4 # bb4) <- bb_uncons bb3 
  (byte5 # bb5) <- bb_uncons bb4 
  (byte6 # bb6) <- bb_uncons bb5 
  (byte7 # bb7) <- bb_uncons bb6 
  (byte8 # bb8) <- bb_uncons bb7 

  let v = 1234  -- TODO: false/dummy conversion of bytes to double! 

  pure1 $ v # bb8


-- TODO: this isn't tail recursive
takeNbytes : HasErr AppHasIO es => Nat -> (1 _ : ByteBlock) -> App1 es (Res (List Bits8) (const ByteBlock))
takeNbytes Z bb = pure1 $ [] # bb
takeNbytes (S n) bb = do
  (v # bb) <- bb_uncons bb
  (rest # bb) <- takeNbytes n bb
  pure1 $ ( [v] ++ rest ) # bb


-- define this signature before the body because we are mutually
-- recursive (or I could use a mutual block? what's the difference?)
step : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_PROTO : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_PROTO bb state = do
  log "Opcode: PROTO"
  -- read a uint1
  (proto_ver # bb') <- bb_uncons bb
  logv "Pickle protocol version" proto_ver
  step bb' state 


step_NEWOBJ : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_NEWOBJ bb (MkVMState (a :: b :: rest) memo) = do
  log "Opcode: NEWOBJ"
  let o = PickleObj a b
  step bb (MkVMState (o :: rest) memo)
step_NEWOBJ _ _ = ?error_NEWOBJ_bad_stack


step_TUPLE2 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_TUPLE2 bb (MkVMState (a::b::rest) memo) = do
  log "Opcode: TUPLE2"
  let new_stack = (PickleTuple [a,b]) :: rest
  -- TODO: this might be the wrong order
  step bb (MkVMState new_stack memo)

step_TUPLE2 _ _ = do
  ?error_bad_stack_for_TUPLE2

step_TUPLE3 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_TUPLE3 bb (MkVMState (a::b::c::rest) memo) = do
  log "Opcode: TUPLE3"
  let new_stack = (PickleTuple [a,b,c]) :: rest
  -- TODO: this might be the wrong order
  step bb (MkVMState new_stack memo)

step_TUPLE3 _ _ = do
  ?error_bad_stack_for_TUPLE3

step_BININT1 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_BININT1 bb (MkVMState stack memo) = do
  log "Opcode: BININT1"
  -- read an unsigned uint1
  (v # bb') <- bb_uncons bb
  logv "1-byte unsigned integer" v
  let new_state = MkVMState ((PickleInteger (cast v))::stack) memo
  step bb' new_state 


step_BININT2 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_BININT2 bb (MkVMState stack memo) = do
  log "Opcode: BININT2"
  (v # bb') <- read_uint2 bb
  logv "2-byte unsigned integer" v
  let new_state = MkVMState ((PickleInteger (cast v))::stack) memo
  step bb' new_state 

step_BININT : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_BININT bb (MkVMState stack memo) = do
  log "Opcode: BININT4"
  (v # bb') <- read_uint4 bb
  logv "4-byte unsigned integer" v
  let new_state = MkVMState ((PickleInteger (cast v))::stack) memo
  step bb' new_state 

step_LONG1 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_LONG1 bb (MkVMState stack memo) = do
  log "Opcode: LONG1"
  (n # bb') <- bb_uncons bb
  logv "LONG1 number of bytes" n
  -- TODO: this is a fake LONG1 parser
  -- TODO: read n bytes even though faking, because otherwise
  --       we lose track of the bytestream
  bytes # bb' <- takeNbytes (the Nat (cast n)) bb'
  let fake = 6789
  
  let new_state = MkVMState ((PickleInteger (cast fake))::stack) memo
  step bb' new_state 

step_BINFLOAT : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState

step_BINFLOAT bb (MkVMState stack memo) = do
  log "Opcode: BINFLOAT"
  (v # bb') <- read_double8 bb
  logv "8-byte float" v
  step bb' (MkVMState ((PickleFloat v)::stack) memo)

step_NONE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_NONE bb (MkVMState stack memo) = do
  log "Opcode: NONE"
  let new_state = MkVMState (PickleNone :: stack) memo
  step bb new_state 

step_BINGET : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_BINGET bb state@(MkVMState stack memo) = do
  log "Opcode: BINGET"
  (memo_slot_bits8 # bb') <- bb_uncons bb
  logv "Retrieving memoization slot" memo_slot_bits8

  let memo_slot = the Nat (cast memo_slot_bits8)

  case inBounds memo_slot memo of
    Yes p => do
      let memo_val = index memo_slot memo
      let state' = MkVMState (memo_val :: stack) memo
      step bb' state'
    No _ => do 
      ?error_memo_index_out_of_bounds
      pure state


step_FRAME : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_FRAME bb state = do
  log "Opcode: FRAME"
  let (n # bb) = length1 bb
  case n of
    (S (S (S (S (S (S (S (S k)))))))) => do
      (frame_len # bb') <- read_uint8 bb
      logv "Frame length is" frame_len

      -- NOTE:
      -- This length used to be inlines as the logv call argument,
      -- using non-linear `length`, but since transition to linear
      -- byte buffers, the returned bb' needs to be bound too, so
      -- can't use function style notation here any more.
      let (bblen # bb') = length1 bb'

      logv "Bytes remaining in buffer" (bblen)
      -- TODO: out of interest, validate FRAME against ByteBlock length.
      -- In pickle in general we can't do that because the input is a
      -- stream, not a fixed length block... but we know the length of
      -- ByteBlock because we're reading the whole thing before attempting
      -- a parse.
      
      step bb' state
    _ => ?error_not_enough_bytes_left_for_FRAME

step_MEMOIZE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_MEMOIZE bb (MkVMState (v::rest_stack) memo) = do
  log "Opcode: MEMOIZE"
  step bb (MkVMState (v::rest_stack) (memo ++ [v]))
step_MEMOIZE bb (MkVMState [] memo) = ?error_MEMOIZE_with_empty_stack

-- Linear typing changes force bb to be deallocated here (or *something* has
-- to be done with it, such as returning it for the next level up to free).
-- > Error: While processing right hand side of step_STOP. There are 0 uses of linear name bb. 
-- This is probably going to reveal bugs in ByteBlock construction where the
-- byte-blocks are not all malloc-allocated due to pointer movement.
step_STOP : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_STOP bb state = do
  log "Opcode: STOP"
  free1 bb
  pure state

step_BINBYTES : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_BINBYTES bb (MkVMState stack memo) = do
  log "Opcode: BINBYTES"
  let (n # bb) = length1 bb
  case n of   -- TODO don't need this case any more because bb_uncons raises a runtime error rather than type-failure
    (S (S (S (S k)))) => do
      (block_len # bb') <- read_uint4 bb
      logv "Byte count" block_len
      -- TODO: what's this foldlM going to look like with a linear Res
      -- threaded through it?
      (bytes # bb'') <- takeNbytes (the Nat (cast block_len)) bb'
      -- fold across a linear resource...
      let new_state = MkVMState ((PickleBytes bytes)::stack) memo

      step bb'' new_state
    _ => ?error_BINBYTES_not_enough_to_count

step_SHORT_BINBYTES : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_SHORT_BINBYTES bb (MkVMState stack memo) = do
  log "Opcode: SHORT_BINBYTES"
  let (n # bb) = length1 bb
  case n of
    (S k) => do
      (block_len # bb') <- bb_uncons bb
      logv "Byte count" block_len
      -- TODO: is there a library function for this?
      (bytes # bb'') <- takeNbytes (the Nat (cast block_len)) bb'
      let new_state = MkVMState ((PickleBytes bytes)::stack) memo

      step bb'' new_state
    _ => ?error_SHORT_BINBYTES_not_enough_to_count


step_SHORT_BINUNICODE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_SHORT_BINUNICODE bb (MkVMState stack memo) = do
  log "Opcode: SHORT_BINUNICODE"
  let (n # bb) = length1 bb
  case n of
    (S k) => do
      (strlen # bb') <- bb_uncons bb
      logv "UTF-8 byte sequence length" strlen

      -- the buffer contains strlen bytes of UTF-8 encoding, which we need to
      -- turn into an Idris2 String. could go via C, or could do it as a
      -- "read byte and append to idris2 string" entirely-in-idris impl?
      -- that latter is a bit more complicated - because a char and a byte are
      -- not the same thing. but we can "read a unicode codepoint" as
      -- a step (read bytes until its not a continuation byte?) and decode
      -- it into a char? that seems like a lot of faff compared to going via
      -- a C buffer...

      (str # bb'') <- str_from_bytes (cast strlen) bb'

      logv "String is" str

      let new_state = MkVMState ((PickleUnicodeString str)::stack) memo

      step bb'' new_state

    Z => ?error_SHORT_BINUNICODE_no_length


step_STACK_GLOBAL : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_STACK_GLOBAL bb (MkVMState (a :: b :: stack) memo) = do
  log "Opcode: STACK_GLOBAL"

  let g = PickleGlobal a b

  let new_state = MkVMState (g :: stack) memo

  step bb new_state 

step_STACK_GLOBAL _ _ = ?error_bad_stack_for_STACK_GLOBAL


step_EMPTYDICT : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_EMPTYDICT bb (MkVMState stack memo) = do
  log "Opcode: EMPTYDICT"

  let new_state = MkVMState ((PickleDict [])::stack) memo

  step bb new_state 

step_EMPTY_TUPLE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_EMPTY_TUPLE bb (MkVMState stack memo) = do
  log "Opcode: EMPTY_TUPLE"

  let new_state = MkVMState ((PickleTuple [])::stack) memo

  step bb new_state 


notMark : PickleAST -> Bool
notMark (PickleMark) = False
notMark _ = True

split_pairs : List a -> List (a, a)
split_pairs [] = []
split_pairs (a :: b :: rest) = (a,b) :: (split_pairs rest)
split_pairs _ = ?error_split_list_not_even
-- TODO: what's a better way to deal with the odd-length
-- case? in App, could raise an exception, but then this function
-- doesn't look very pure. (which is true... it isn't, because it
-- isn't covering all lists). or `Maybe`. or require a proof of
-- even-ness and use that proof to convince idris of coveringness?
-- which pushes the error check one level higher.

-- TODO: this mutates an object, rather than being value-pure, which will
-- interact incorrectly with values stored by MEMOIZE: the modification
-- should be visible in the MEMOIZED object, but won't be. In the parsl
-- case, that's probably ok, because I don't think memoized objects are
-- every used in the interchange (and the memoize retrieval opcodes are not
-- implemented...)
step_SETITEMS : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_SETITEMS bb (MkVMState stack memo) = do
  log "Opcode: SETITEMS"

  -- get a stack slice from the start of the stack (most recent) down to the
  -- first MARK object. All of those should then disappear from the stack.

  -- If the state moves to being
  -- an App-state, then it would become monadic perform-action-while, which
  -- would keep the rest_stack implicit
  let (items, rest) = span notMark stack
  case rest of
    (PickleMark :: dict@(PickleDict old_items) :: rest_stack) => do
      logv "Item pairs" items
      logv "Dict" dict
      logv "Rest of stack" rest_stack

      -- now set item pairs from `items`, starting lowest down the stack
      -- (so at the end of the items list)

      let item_pairs = map (\(a,b) => (b,a)) $ split_pairs items
      logv "item pairs" item_pairs

      -- What k/v data structure to use? Language.JSON stores json objects
      -- as a list of k/v tuples. So I'll do that here too. It's not super
      -- efficient but I'm expecting dictionaries of size 2 in the interchange.

      -- TODO: for key in k/v there is some Python-level equivalence to be
      -- used for the keys. In almost every situation, I don't imagine a
      -- pickle setting a key/value then in the same program setting the
      -- same key to a different value, but it is a possibility I think
      -- (at least the SETITEMS pickletools doc talks about ordering of
      -- setting operations, which would be relevant mostly in the k1=k2 case)

      let new_stack = (PickleDict (item_pairs ++ old_items)) :: rest_stack

      step bb (MkVMState new_stack memo)
    _ => ?error_stack_malformed_for_SETITEMS


step_SETITEM : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_SETITEM bb (MkVMState stack memo) = do
  log "Opcode: SETITEM"

  -- TODO: there's better let/alternative syntax here that I can't remember
  case stack of
    (v :: k :: (PickleDict old_items) :: rest_stack) => do
      let new_stack = (PickleDict ([(k, v)] ++ old_items)) :: rest_stack
      step bb (MkVMState new_stack memo)
    _ => ?error_stack_malformed_for_SETITEM


step_MARK : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> VMState -> App1 {u=Any} es VMState
step_MARK bb (MkVMState stack memo) = do
  log "Opcode: MARK"

  let new_state = MkVMState (PickleMark::stack) memo

  step bb new_state 

step bb state = do

    logv "Stack pre-step" state.stack

    (opcode # bb') <- bb_uncons bb

    logv "Opcode number" opcode

    case opcode of 
      40 => step_MARK bb' state
      41 => step_EMPTY_TUPLE bb' state
      46 => step_STOP bb' state
      66 => step_BINBYTES bb' state
      67 => step_SHORT_BINBYTES bb' state
      71 => step_BINFLOAT bb' state
      74 => step_BININT bb' state
      75 => step_BININT1 bb' state
      77 => step_BININT2 bb' state
      78 => step_NONE bb' state
      104 => step_BINGET bb' state
      115 => step_SETITEM bb' state
      117 => step_SETITEMS bb' state
      125 => step_EMPTYDICT bb' state
      128 => step_PROTO bb' state
      129 => step_NEWOBJ bb' state
      134 => step_TUPLE2 bb' state
      135 => step_TUPLE3 bb' state
      138 => step_LONG1 bb' state
      140 => step_SHORT_BINUNICODE bb' state
      147 => step_STACK_GLOBAL bb' state
      148 => step_MEMOIZE bb' state
      149 => step_FRAME bb' state
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
unpickle : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 {u=Any} es PickleAST
unpickle bb = do
  log "beginning unpickle"

  let init_vm_state = MkVMState [] []

  (MkVMState (stack_head::rest) end_memo) <- step bb init_vm_state
    | _ => ?error_stack_was_empty_at_end

  log "done with unpickle"
  pure stack_head

pickle_PROTO : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> Nat -> App1 es ByteBlock
pickle_PROTO bytes v = do
  log "Pickling PROTO opcode"
  -- write PROTO byte
  -- write v as a byte

  -- can I do this symmetrically to bb_uncons, but appending
  -- at the end rather than taking off the front? i.e. treat
  -- ByteBlock as kinda looking like a stream with take off
  -- the front, append on the end... that probably fits the
  -- right sort of UI for a pickle buffer? (there's no need
  -- for the constructor to be a mirror of the deconstructor
  -- because this isn't an algebraic data type?)
  -- appending on the end probably maps ok-ish onto the
  -- API for realloc? except danger here! realloc can move
  -- the memory block, making any previous ByteBlocks that
  -- point into that space invalid... which is then maybe
  -- an interesting use for linear behaviour...
  -- my coding style so far has been quite linear with ByteBlocks
  -- so I wonder how easy/hard that will be to make into a
  -- linear style? also see that blog post on linear types with
  -- holes from tweag...
  -- so lets take the dangerous (without linear types) route
  -- and do reallocs...

  bytes <- bb_append bytes 128   -- PROTO=128
  bytes <- bb_append bytes (cast v)

  pure1 bytes

pickle_ast : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> PickleAST -> App1 es ByteBlock

pickle_AST_list : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> List PickleAST -> App1 es ByteBlock
pickle_AST_list bb [] = pure1 bb
pickle_AST_list bb (ast::rest) = do
  bb <- pickle_ast bb ast
  pickle_AST_list bb rest

pickle_STOP : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 es ByteBlock
pickle_STOP bytes = do
  log "Pickling STOP opcode"
  bytes <- bb_append bytes 46  -- STOP is ASCII 46
  pure1 bytes


-- this is the same code as pickle_TUPLE except for the opcode
pickle_LIST : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> List PickleAST -> App1 es ByteBlock
pickle_LIST bytes entries = do
  log "Pickling LIST"
  bytes <- bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40
  bytes <- pickle_AST_list bytes entries
  bytes <- bb_append bytes 108  -- opcode is ASCII l
  pure1 bytes


pickle_TUPLE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> List PickleAST -> App1 es ByteBlock
pickle_TUPLE bytes entries = do
  log "Pickling TUPLE"
  bytes <- bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40

  -- Do some kind of fold over the entries list, to generate
  -- a stack section that contains all those entries.
  bytes <- pickle_AST_list bytes entries
  
  bytes <- bb_append bytes 116  -- opcode is ASCII t, decimal 116

  pure1 bytes

-- TODO: byte block contains n at runtime ... so maybe its possible
-- to rearrange this dependent pair uses to treat ByteBlock as the
-- dependant pair of n and ptr? i guess in that case, the n is not
-- fixed - it can be anything - that's to point of the dependent
-- pair? to say *who* can define that. If we were reasoning about
-- ByteBlock in the same way, then n wouldn't be included as a
-- type paramter, just like its not a type parameter but instead
-- explicitly chosen, in (n ** ByteBlock n)  ?

-- The first AST that we need to pickle is this, the initial worker
-- ports response:
--   PickleTuple [PickleInteger 9000, PickleInteger 9001]

store_INT : HasErr AppHasIO es => (1 _ : ByteBlock) -> Int -> App1 es ByteBlock
store_INT bytes v = do
  let b1 = v `mod` 256
  let b2 = (v `div` 256) `mod` 256
  let b3 = (v `div` 256 `div` 256) `mod` 256
  let b4 = (v `div` 256 `div` 256 `div` 256)

  -- if v is too big, this will overflow a Bits8...
  -- I'm not sure if thats a silent or raising error?
  -- dividing b5 to give a b5, and checking b5 is 0
  -- would be a test for that

  bytes <- bb_append bytes (cast b1)
  bytes <- bb_append bytes (cast b2)
  bytes <- bb_append bytes (cast b3)
  bytes <- bb_append bytes (cast b4)
  pure1 bytes

pickle_BININT : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> Int -> App1 es ByteBlock
pickle_BININT bytes v = do
  logv "Pickling BININT" v

  if v < 0 then ?notimpl_BININT_negatives
           else log "this isn't negative - ok"
  bytes <- bb_append bytes 74   -- opcode is ASCII 'J'
  bytes <- store_INT bytes v
  pure1 bytes

store_LONG1_8 : HasErr AppHasIO es => (1 _ : ByteBlock) -> Integer -> App1 es ByteBlock
store_LONG1_8 bytes v = do
  let b1 = v `mod` 256
  let b2 = (v `div` 256) `mod` 256
  let b3 = (v `div` 256 `div` 256) `mod` 256
  let b4 = (v `div` 256 `div` 256 `div` 256) `mod` 256
  let b5 = (v `div` 256 `div` 256 `div` 256 `div` 256) `mod` 256
  let b6 = (v `div` 256 `div` 256 `div` 256 `div` 256 `div` 256) `mod` 256
  let b7 = (v `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256) `mod` 256
  let b8 = (v `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256) `mod` 256
  let b9 = (v `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256 `div` 256) `mod` 256

  -- ISSUE: this is `when` but I get a cannot find Applicative instance error
  if (b9 /= 0) 
   then ?error_store_LONG1_8_b9_not_zero
   else pure ()

  bytes <- bb_append bytes (cast b1)
  bytes <- bb_append bytes (cast b2)
  bytes <- bb_append bytes (cast b3)
  bytes <- bb_append bytes (cast b4)
  bytes <- bb_append bytes (cast b5)
  bytes <- bb_append bytes (cast b6)
  bytes <- bb_append bytes (cast b7)
  bytes <- bb_append bytes (cast b8)

  pure1 bytes



||| This is not a full implementation of LONG1: that can be up to 256 bytes of number
||| but here I am using it to represent only (and always) 8 bytes of number - even
||| if this could fit in fewer bytes.
pickle_LONG1 : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> Integer -> App1 es ByteBlock
pickle_LONG1 bytes v = do
  logv "Pickling LONG1" v
  bytes <- bb_append bytes 138   -- opcode is \x8a
  bytes <- bb_append bytes 8 -- constant 8 bytes number size now - TODO full LONG1 impl would allow bigger
  bytes <- store_LONG1_8 bytes v
  pure1 bytes

pickle_integer : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> Integer -> App1 es ByteBlock
pickle_integer bytes v = do
  logv "Pickling integer" v

  -- this cast (from integer to int) works because v is small. there's no type checking
  -- that the if clause matches the capabilities of the case, though...
  if v < 2^31
   then pickle_BININT bytes (cast v) 
   else if v < 2^63 then pickle_LONG1 bytes v
                    else ?notimpl_pickle_integer_toobig

pickle_DICT_entries : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> List (PickleAST, PickleAST) -> App1 es ByteBlock
pickle_DICT_entries bb [] = pure1 bb
pickle_DICT_entries bb ((ast1, ast2)::rest) = do
  logv "Pickling over DICT entry with key" ast1
  bb <- pickle_ast bb ast1
  bb <- pickle_ast bb ast2
  pickle_DICT_entries bb rest

pickle_DICT : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> List (PickleAST, PickleAST) -> App1 es ByteBlock
pickle_DICT bytes entries = do
  log "Pickling DICT"

  -- so what does a dict pickle look like?
  -- according to Lib/pickletools.py:
  --   pydict markobject key_1 value_1 ... key_n value_n

  bytes <- bb_append bytes 125  -- EMPTY_DICT opcode is ASCII '}', decimal 125

  bytes <- bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40

  bytes <- pickle_DICT_entries bytes entries

  bytes <- bb_append bytes 117  -- SETITEMS opcode is ASCII 'u'
  pure1 bytes


pickle_UNICODE : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> String -> App1 es ByteBlock
pickle_UNICODE bytes s = do
  bytes <- bb_append bytes 140  -- SHORT_BINUNICODE - single byte for length
  sbytes <- bytes_from_str s
  let (slen # sbytes) = length1 sbytes
  bytes <- bb_append bytes (cast slen) -- TODO no check for slen overflowing in this cast
  bytes <- bb_append_bytes bytes sbytes
  pure1 bytes

fold_bytes : HasErr AppHasIO es => (1 _ : ByteBlock) -> List Bits8 -> App1 es ByteBlock
fold_bytes bytes [] = pure1 bytes
fold_bytes bytes (b::rest) = do
  bytes <- bb_append bytes b
  fold_bytes bytes rest

pickle_BYTES : HasErr AppHasIO es => (1 _ : ByteBlock) -> List Bits8 -> App1 es ByteBlock
pickle_BYTES bytes b = do
  bytes <- bb_append bytes 66  -- BINBYTES, opcode ASCII 'B'
  bytes <- store_INT bytes ((cast . length) b)
  fold_bytes bytes b

pickle_ast bytes (PickleTuple elements) = pickle_TUPLE bytes elements
pickle_ast bytes (PickleList elements) = pickle_LIST bytes elements
pickle_ast bytes (PickleInteger v) = pickle_integer bytes v
pickle_ast bytes (PickleDict l) = pickle_DICT bytes l
pickle_ast bytes (PickleUnicodeString s) = pickle_UNICODE bytes s
pickle_ast bytes (PickleBytes b) = pickle_BYTES bytes b
pickle_ast _ _ = ?notimpl_pickle_ast_others

||| Takes some PickleAST and turns it into a Pickle bytestream.
export
pickle : (State LogConfig LogConfig es, HasErr AppHasIO es) => PickleAST -> App1 es ByteBlock
pickle ast = do
  -- TODO: needs some kinds of ByteBlock access that we can write to in
  -- the ways that this code wants. I think that only means appending,
  -- because generally we will write out the args one by one (recursively)
  -- and then the relevant opcode and immediate arguments.

  -- 0-byte byte blocks seem a bit awkward in C land because malloc can return
  -- NULL if called with 0 size, so this would need some special casing?
  -- would be nice to have a monoidal structure (but in IO, I guess because
  -- of allocs? unless i start doing things with the ffi callback to free
  -- unused objects? which might fit here so that stuff is monoidal without
  -- any explicit effects (IO, linearity) ?) -- but I'd probably like to aim
  -- eventually for something linear to try to control resource usage rather
  -- than doing GC?

  -- PROTO 4
  proto_header_bytes <- pickle_PROTO emptyByteBlock 4

  --   cpython generated pickles then have a FRAME, but I think this isn't
  --   compulsory - although it might cause some performance degredation in
  --   streaming cases, I think in bytes-from-zmq cases like Parsl it won't
  --   matter as the block of bytes has already arrived and been placed into
  --   memory...

  -- then recursively run per-PickleAST-constructor code.

  ast_bytes <- pickle_ast proto_header_bytes ast

  complete_bytes <- pickle_STOP ast_bytes

  pure1 complete_bytes
