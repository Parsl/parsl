||| enough of pickle to make the interchange work, no more...
|||
||| makes heavy reference to Lib/pickletools.py (from around Python 3.12)
module Pickle

import Control.App
import Generics.Derive

import Bytes
import Logging

%language ElabReflection

-- It should be possible for this to be total, on the length of the
-- block of bytes, but this doesn't work at time of writing: I guess
-- I haven't conveyed that the recursion terminates properly.
-- %default total

||| some untyped (or Python Any-typed) representation of the output
||| of executing a pickle.
||| PickleMark is a special thing here because its used on the stack
||| but not expected to be exposed as part of the final value.
public export
data PickleAST = PickleUnicodeString String
               | PickleInteger Int
               | PickleTuple (List PickleAST)
               | PickleList (List PickleAST)
               | PickleDict (List (PickleAST, PickleAST))
               | PickleMark
               | PickleBytes (List Bits8)
%runElab derive "PickleAST" [Generic, Meta, Show]

record VMState where
  constructor MkVMState
  stack: List PickleAST
  memo: List PickleAST  -- sequential map of integers to objects - memo key n is at position n in the list so we don't need to store the key


-- read an 8-byte integer - uint8 is the pickletools ArgumentDescriptor name.
-- if there are not 8 bytes left in ByteBlock, explode...
-- TODO: this could look nicer with a parser monad/parser applicative
-- rather than threading the shrinking bb?
read_uint8 : HasErr AppHasIO es => {n: Nat} -> ByteBlock (S (S (S (S (S (S (S (S n)))))))) -> App es (Int, ByteBlock n)
read_uint8 bb = do
  (byte1, bb1) <- primIO $ bb_uncons bb 
  (byte2, bb2) <- primIO $ bb_uncons bb1 
  (byte3, bb3) <- primIO $ bb_uncons bb2
  (byte4, bb4) <- primIO $ bb_uncons bb3 
  (byte5, bb5) <- primIO $ bb_uncons bb4 
  (byte6, bb6) <- primIO $ bb_uncons bb5 
  (byte7, bb7) <- primIO $ bb_uncons bb6 
  (byte8, bb8) <- primIO $ bb_uncons bb7 

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
  pure (v, bb8)


read_uint4 : HasErr AppHasIO es => {n: Nat} -> ByteBlock (S (S (S (S n)))) -> App es (Int, ByteBlock n)
read_uint4 bb = do
  (byte1, bb1) <- primIO $ bb_uncons bb 
  (byte2, bb2) <- primIO $ bb_uncons bb1 
  (byte3, bb3) <- primIO $ bb_uncons bb2
  (byte4, bb4) <- primIO $ bb_uncons bb3 

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
  pure (v, bb4)



-- define this signature before the body because we are mutually
-- recursive (or I could use a mutual block? what's the difference?)
step : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState

step_PROTO : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState

step_PROTO {n = Z} bb state = do
  ?error_PROTO_ran_off_end
  pure state

step_PROTO {n = S n'} bb state = do
  log "Opcode: PROTO"
  -- read a uint1
  (proto_ver, bb') <- primIO $ bb_uncons bb
  logv "Pickle protocol version" proto_ver
  step {n = n'} bb' state 

step_BININT1 : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState

step_BININT1 {n = Z} bb state = do
  ?error_BININT1_ran_off_end
  pure state
step_BININT1 {n = S n'} bb (MkVMState stack memo) = do
  log "Opcode: BININT1"
  -- read an unsigned uint1
  (v, bb') <- primIO $ bb_uncons bb
  logv "1-byte unsigned integer" v
  let new_state = MkVMState ((PickleInteger (cast v))::stack) memo
  step {n = n'} bb' new_state 

step_BINGET : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState
step_BINGET {n = Z} _ _ = ?error_BINGET_out_of_data
step_BINGET {n = S n'} bb state@(MkVMState stack memo) = do
  log "Opcode: BINGET"
  (memo_slot_bits8, bb') <- primIO $ bb_uncons bb
  logv "Retrieving memoization slot" memo_slot_bits8

  let memo_slot = the Nat (cast memo_slot_bits8)

  case inBounds memo_slot memo of
    Yes p => do
      let memo_val = index memo_slot memo
      let state' = MkVMState (memo_val :: stack) memo
      step {n = n'} bb' state'
    No _ => do 
      ?error_memo_index_out_of_bounds
      pure state


step_FRAME : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState
step_FRAME bb state = do
  log "Opcode: FRAME"
  case n of
    (S (S (S (S (S (S (S (S k)))))))) => do
      (frame_len, bb') <- read_uint8 bb
      logv "Frame length is" frame_len
      logv "Bytes remaining in buffer" (length bb')
      -- TODO: out of interest, validate FRAME against ByteBlock length.
      -- In pickle in general we can't do that because the input is a
      -- stream, not a fixed length block... but we know the length of
      -- ByteBlock because we're reading the whole thing before attempting
      -- a parse.
      
      step bb' state
    _ => ?error_not_enough_bytes_left_for_FRAME

step_MEMOIZE : HasErr AppHasIO es => {n: Nat} -> ByteBlock n -> VMState -> App es VMState
step_MEMOIZE bb (MkVMState (v::rest_stack) memo) = do
  log "Opcode: MEMOIZE"
  step bb (MkVMState (v::rest_stack) (memo ++ [v]))
step_MEMOIZE bb (MkVMState [] memo) = ?error_MEMOIZE_with_empty_stack


step_STOP : HasErr AppHasIO es => ByteBlock n -> VMState -> App es VMState
step_STOP bb state = do
  log "Opcode: STOP"
  pure state


binbytes_folder : HasErr AppHasIO es => (List Bits8, (n : Nat ** ByteBlock n)) -> Int -> App es (List Bits8, (m : Nat ** ByteBlock m))
binbytes_folder (l, (k ** bb)) _ = do
  case k of
    (S k') => do
      (v, bb') <- primIO $ bb_uncons bb
      pure (l ++ [v], (k' ** bb'))
    _ => ?error_binbytes_folder_exhausted_bytes

step_BINBYTES : HasErr AppHasIO es => {n: Nat} -> ByteBlock n -> VMState -> App es VMState
step_BINBYTES {n} bb (MkVMState stack memo) = do
  log "Opcode: BINBYTES"
  case n of
    (S (S (S (S k)))) => do
      (block_len, bb') <- read_uint4 bb
      logv "Byte count" block_len
      -- TODO: is there a library function for this?
      (bytes, (l ** bb'')) <- foldlM binbytes_folder ([], (k ** bb')) [1..block_len] 
      let new_state = MkVMState ((PickleBytes bytes)::stack) memo

      step bb'' new_state
    _ => ?error_BINBYTES_not_enough_to_count

-- The reasoning about lengths here is more complicated than PROTO or FRAME,
-- and maybe pushes more into runtime: the number of bytes we want is encoded
--  in the first remaining byte of the ByteBlock.
step_SHORT_BINUNICODE : HasErr AppHasIO es => {n: Nat} -> ByteBlock n -> VMState -> App es VMState
step_SHORT_BINUNICODE {n} bb (MkVMState stack memo) = do
  log "Opcode: SHORT_BINUNICODE"
  case n of
    (S k) => do
      (strlen, bb') <- primIO $ bb_uncons bb
      logv "UTF-8 byte sequence length" strlen

      -- the buffer contains strlen bytes of UTF-8 encoding, which we need to
      -- turn into an Idris2 String. could go via C, or could do it as a
      -- "read byte and append to idris2 string" entirely-in-idris impl?
      -- that latter is a bit more complicated - because a char and a byte are
      -- not the same thing. but we can "read a unicode codepoint" as
      -- a step (read bytes until its not a continuation byte?) and decode
      -- it into a char? that seems like a lot of faff compared to going via
      -- a C buffer...

      (str, bb'') <- primIO $ str_from_bytes (cast strlen) bb'

      logv "String is" str

      let new_state = MkVMState ((PickleUnicodeString str)::stack) memo

      step bb'' new_state

    Z => ?error_SHORT_BINUNICODE_no_length

step_EMPTYDICT : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState
step_EMPTYDICT {n} bb (MkVMState stack memo) = do
  log "Opcode: EMPTYDICT"

  let new_state = MkVMState ((PickleDict [])::stack) memo

  step {n} bb new_state 

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
step_SETITEMS : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState
step_SETITEMS {n} bb (MkVMState stack memo) = do
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

      step {n} bb (MkVMState new_stack memo)
    _ => ?error_stack_malformed_for_SETITEMS

step_MARK : HasErr AppHasIO es => {n : Nat} -> ByteBlock n -> VMState -> App es VMState
step_MARK {n} bb (MkVMState stack memo) = do
  log "Opcode: MARK"

  let new_state = MkVMState (PickleMark::stack) memo

  step {n} bb new_state 

step {n = Z} bb state = do
    log "ERROR: Pickle VM ran off end of pickle"
    ?error_pickle_ran_off_end

step {n = S m} bb state = do

    logv "Stack pre-step" state.stack

    (opcode, bb') <- primIO $ bb_uncons bb

    logv "Opcode number" opcode

    case opcode of 
      40 => step_MARK bb' state
      46 => step_STOP bb' state
      66 => step_BINBYTES bb' state
      75 => step_BININT1 bb' state
      104 => step_BINGET bb' state
      117 => step_SETITEMS bb' state
      125 => step_EMPTYDICT bb' state
      128 => step_PROTO {n = m} bb' state
      140 => step_SHORT_BINUNICODE bb' state
      148 => step_MEMOIZE bb' state
      149 => step_FRAME {n = m} bb' state
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
unpickle : HasErr AppHasIO es => (n: Nat ** (ByteBlock n)) -> App es PickleAST
unpickle ((S n) ** bb) = do
  log "beginning unpickle"

  let init_vm_state = MkVMState [] []

  (MkVMState (stack_head::rest) end_memo) <- step bb init_vm_state
    | _ => ?error_stack_was_empty_at_end

  log "done with unpickle"
  pure stack_head

unpickle (Z ** bb) = do
  log "unpickle not defined on empty byte sequence"
  ?error_unpickle_Z

pickle_PROTO : HasErr AppHasIO es => (n ** ByteBlock n) -> Nat -> App es (m ** ByteBlock m)
pickle_PROTO (n ** bytes) v = do
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

  bytes <- primIO $ bb_append bytes 128   -- PROTO=128
  bytes <- primIO $ bb_append bytes (cast v)

  pure ((S (S n)) ** bytes)

pickle_ast : HasErr AppHasIO es => (n: Nat ** ByteBlock n) -> PickleAST -> App es (m: Nat ** (ByteBlock m))

fold_AST : HasErr AppHasIO es => (n ** ByteBlock n) -> PickleAST -> App es (m ** ByteBlock m)
fold_AST (n ** bytes) ast = do
  log "Folding over an AST element"
  pickle_ast (n ** bytes) ast

pickle_STOP : HasErr AppHasIO es => (n ** ByteBlock n) -> App es (m ** ByteBlock m)
pickle_STOP (n ** bytes) = do
  log "Pickling STOP opcode"
  bytes <- primIO $ bb_append bytes 46  -- STOP is ASCII 46
  pure ((S n) ** bytes)


-- this is the same code as pickle_TUPLE except for the opcode
pickle_LIST : HasErr AppHasIO es => (n ** ByteBlock n) -> List PickleAST -> App es (m ** ByteBlock m)
pickle_LIST (n ** bytes) entries = do
  log "Pickling LIST"
  bytes <- primIO $ bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40
  (len ** bytes) <- foldlM fold_AST ((S n) ** bytes) entries
  bytes <- primIO $ bb_append bytes 108  -- opcode is ASCII l
  pure ((S len) ** bytes)


pickle_TUPLE : HasErr AppHasIO es => (n ** ByteBlock n) -> List PickleAST -> App es (m ** ByteBlock m)
pickle_TUPLE (n ** bytes) entries = do
  log "Pickling TUPLE"
  bytes <- primIO $ bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40

  -- Do some kind of fold over the entries list, to generate
  -- a stack section that contains all those entries.
  (len ** bytes) <- foldlM fold_AST ((S n) ** bytes) entries
  
  bytes <- primIO $ bb_append bytes 116  -- opcode is ASCII t, decimal 116

  pure ((S len) ** bytes)

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

store_INT : HasErr AppHasIO es => (n ** ByteBlock n) -> Int -> App es (m ** ByteBlock m)
store_INT (n ** bytes) v = do
  let b1 = v `mod` 256
  let b2 = (v `div` 256) `mod` 256
  let b3 = (v `div` 256 `div` 256) `mod` 256
  let b4 = (v `div` 256 `div` 256 `div` 256)

  -- if v is too big, this will overflow a Bits8...
  -- I'm not sure if thats a silent or raising error?
  -- dividing b5 to give a b5, and checking b5 is 0
  -- would be a test for that

  bytes <- primIO $ bb_append bytes (cast b1)
  bytes <- primIO $ bb_append bytes (cast b2)
  bytes <- primIO $ bb_append bytes (cast b3)
  bytes <- primIO $ bb_append bytes (cast b4)
  pure (S (S (S (S n))) ** bytes)


pickle_BININT : HasErr AppHasIO es => (n ** ByteBlock n) -> Int -> App es (m ** ByteBlock m)
pickle_BININT (n ** bytes) v = do
  logv "Pickling BININT" v

  if v < 0 then ?notimpl_BININT_negatives
           else log "this isn't negative - ok"
  bytes <- primIO $ bb_append bytes 74   -- opcode is ASCII 'J'
  (m ** bytes) <- store_INT (S n ** bytes) v
  pure (m ** bytes)

fold_DICT_entry : HasErr AppHasIO es => (n ** ByteBlock n) -> (PickleAST, PickleAST) -> App es (m ** ByteBlock m)
fold_DICT_entry (n ** bytes) (ast1, ast2) = do
  log "Folding over DICT element"
  (n' ** bytes') <- pickle_ast (n ** bytes) ast1
  pickle_ast (n' ** bytes') ast2

pickle_DICT : HasErr AppHasIO es => (n ** ByteBlock n) -> List (PickleAST, PickleAST) -> App es (m ** ByteBlock m)
pickle_DICT (n ** bytes) entries = do
  log "Pickling DICT"

  -- so what does a dict pickle look like?
  -- according to Lib/pickletools.py:
  --   pydict markobject key_1 value_1 ... key_n value_n

  bytes <- primIO $ bb_append bytes 125  -- EMPTY_DICT opcode is ASCII '}', decimal 125

  bytes <- primIO $ bb_append bytes 40  -- MARK opcode is ASCII '(', decimal 40

  (len ** bytes) <- foldlM fold_DICT_entry ((S (S n)) ** bytes) entries

  bytes <- primIO $ bb_append bytes 117  -- SETITEMS opcode is ASCII 'u'
  pure ((S len) ** bytes)


pickle_UNICODE : HasErr AppHasIO es => (n ** ByteBlock n) -> String -> App es (m ** ByteBlock m)
pickle_UNICODE (n ** bytes) s = do
  bytes <- primIO $ bb_append bytes 140  -- SHORT_BINUNICODE - single byte for length
  sbytes@(slen ** _) <- primIO $ bytes_from_str s
  bytes <- primIO $ bb_append bytes (cast slen) -- TODO no check for slen overflowing in this cast
  (l ** bytes) <- primIO $ bb_append_bytes bytes sbytes
  pure (l ** bytes)

fold_byte : HasErr AppHasIO es => (n ** ByteBlock n) -> Bits8 -> App es (m ** ByteBlock m)
fold_byte (n ** bytes) b = do
  bytes <- primIO $ bb_append bytes b
  pure ((S n) ** bytes)

pickle_BYTES : HasErr AppHasIO es => (n ** ByteBlock n) -> List Bits8 -> App es (m ** ByteBlock m)
pickle_BYTES (n ** bytes) b = do
  bytes <- primIO $ bb_append bytes 66  -- BINBYTES, opcode ASCII 'B'
  bytes <- store_INT ((S n) ** bytes) ((cast . length) b)
  foldlM fold_byte bytes b

pickle_ast bytes (PickleTuple elements) = pickle_TUPLE bytes elements
pickle_ast bytes (PickleList elements) = pickle_LIST bytes elements
pickle_ast bytes (PickleInteger v) = pickle_BININT bytes v
pickle_ast bytes (PickleDict l) = pickle_DICT bytes l
pickle_ast bytes (PickleUnicodeString s) = pickle_UNICODE bytes s
pickle_ast bytes (PickleBytes b) = pickle_BYTES bytes b
pickle_ast _ _ = ?notimpl_pickle_ast_others

||| Takes some PickleAST and turns it into a Pickle bytestream.
export
pickle : HasErr AppHasIO es => PickleAST -> App es (n: Nat ** (ByteBlock n))
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
  proto_header_bytes <- pickle_PROTO (0 ** emptyByteBlock) 4

  --   cpython generated pickles then have a FRAME, but I think this isn't
  --   compulsory - although it might cause some performance degredation in
  --   streaming cases, I think in bytes-from-zmq cases like Parsl it won't
  --   matter as the block of bytes has already arrived and been placed into
  --   memory...

  -- then recursively run per-PickleAST-constructor code.

  ast_bytes <- pickle_ast proto_header_bytes ast

  complete_bytes <- pickle_STOP ast_bytes

  pure complete_bytes
