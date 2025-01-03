||| Kinda like Data.Buffer but also not...
||| This represents a buffer of data that lives in an FFI memory
||| allocation, with a length. The intention is to represent the
||| message piece of a zmq_msg_t for the purposes of parsing it
||| using a pickle decoder.
||| I would like access to it to be linearly typed (eg with
||| App1) to enforce memory freeing.
module Bytes

import Control.App

-- can't be total because primIO appears to not be total?
-- when this was in IO, this wasn't a problem but moving it to
-- AppHasIO breaks totality...
-- Error: bb_uncons is not total, possibly not terminating due to function Control.App.PrimIO at Control.App:353:1--359:62 being reachable via Control.App.PrimIO implementation at Control.App:361:1--377:35 -> Control.App.PrimIO at Control.App:353:1--359:62
-- That's a shame because I'd hoped that I could have total rather than covering definitions for more stuff here...
-- but maybe I don't have to use IO here? the IO is to do primIO FFI stuff, but FFI doesn't have to be done within IO - perhaps theres some linear types to be done here instead?

-- %default total

public export
data ByteBlock = MkByteBlock AnyPtr Nat


-- i think this doesn't need to be in IO because its
-- not doing anything that allocates resources: it
-- uses a NULL pointer for the memory allocation, and
-- because it is length 0, nothing should ever
-- dereference that, and nothing needs to free it.
-- But if something *does* free it, that's fine, because
-- libc free() will take a NULL pointer without harm.
export
emptyByteBlock : ByteBlock
emptyByteBlock = MkByteBlock prim__getNullAnyPtr 0

%foreign "C:readByteAt,bytes"
prim__readByteAt : AnyPtr -> PrimIO Bits8

%foreign "C:incPtrBy,bytes"
prim__incPtrBy : Int -> AnyPtr -> AnyPtr

incPtr : AnyPtr -> AnyPtr
incPtr p = prim__incPtrBy 1 p

incPtrBy : Int -> AnyPtr -> AnyPtr
incPtrBy n p = prim__incPtrBy n p

export
bb_uncons : HasErr AppHasIO es => (1 _ : ByteBlock) -> App1 es (Res Bits8 (const ByteBlock))
bb_uncons (MkByteBlock ptr (S n)) = do

  v <- app $ primIO $ primIO $ prim__readByteAt ptr

  let ptr_inc = incPtr ptr
  let rest = MkByteBlock ptr_inc n

  pure1 (v # rest)

bb_uncons (MkByteBlock _ Z) = ?error_unconsing_from_empty_byteblock


%foreign "C:copy_and_append,bytes"
prim__copy_and_append: (1 _ : AnyPtr) -> Int -> Bits8 -> AnyPtr

export
bb_append : HasErr AppHasIO es => (1 _ : ByteBlock) -> Bits8 -> App1 es ByteBlock
bb_append (MkByteBlock ptr n) v = do
  -- can't necessarily realloc here, because ptr is not
  -- necessarily a malloced ptr: it might be a pointer
  -- further along into the block... and without any 
  -- linearity we don't have any guarantee about other
  -- ByteBlocks sharing the same underlying memory...
  -- (which is also a problem for arbitrary mutability)
  let new_ptr = prim__copy_and_append ptr (cast n) v
  pure1 (MkByteBlock (new_ptr) (S n))


export
length : ByteBlock -> Nat
length (MkByteBlock _ l) = l

export
length1 : (1 _ : ByteBlock) -> Res Nat (const ByteBlock)
length1 (MkByteBlock p l) = l # (MkByteBlock p l)


%foreign "C:free,libc"
prim__free : (1 _ : AnyPtr) -> ()

public export
free : (1 _ : ByteBlock) -> App {l = NoThrow} es ()
free (MkByteBlock p n) = pure $ prim__free p

public export
free1 : (1 _ : ByteBlock) -> App1 {u=Any} es ()
free1 bb = app $ free bb

covering export
bb_append_bytes : HasErr AppHasIO es => (1 _ : ByteBlock) -> (1 _ : ByteBlock) -> App1 es ByteBlock
bb_append_bytes a b = do

  let (m # b') = length1 b

  case m of
    Z => do
      app $ free b'
      pure1 a   -- need to use b in this path -- i guess can free it? even though because of Z, we should know that there is no ptr - that fact isn't type-encoded, but it could be if using two constructors? using free here requires that ptr came from a malloc-style allocation directly.
    S x => do
      (v # rest) <- bb_uncons b'
      a' <- bb_append a v
      bb_append_bytes a' rest
    
%foreign "C:str_from_bytes,bytes"
prim__str_from_bytes : Int -> AnyPtr -> PrimIO String

export
str_from_bytes : HasErr AppHasIO es => Nat -> (1 _ : ByteBlock) -> App1 es (Res String (const ByteBlock))
str_from_bytes l (MkByteBlock p l') = do
  s <- app $ primIO $ primIO $ prim__str_from_bytes (cast l) p
  let rest = MkByteBlock (incPtrBy (cast l) p) (l' `minus` l)
  pure1 $ s # rest
  -- TODO: who unallocates the malloc in prim__str_from_bytes?

%foreign "C:unicode_byte_len,bytes"
prim__unicode_byte_len : String -> PrimIO Int

%foreign "C:unicode_bytes,bytes"
prim__unicode_bytes : String -> PrimIO AnyPtr

export
bytes_from_str : HasErr AppHasIO es => String -> App1 es ByteBlock
bytes_from_str s = do
  len <- app $ primIO $ primIO $ prim__unicode_byte_len s
  strbytes <- app $ primIO $ primIO $ prim__unicode_bytes s
  pure1 $ MkByteBlock strbytes (cast len)

%foreign "C:duplicate_block,bytes"
prim__duplicate_block : AnyPtr -> Int -> PrimIO AnyPtr

-- it would be interesting to figure out if I can express type-safe sharing of data
-- rather than having to copy it here - eg type level counting of references or
-- something like that?
-- for example, a reference counted read-only structure (at runtime) and we use linear types
-- to statically assure that the *reference count* is released, rather than the
-- memory block itself (with the memory being released when the count hits 0
-- dynamically?)
export
bb_duplicate : HasErr AppHasIO es => (1 _ : ByteBlock) -> App1 es (LPair ByteBlock ByteBlock)
bb_duplicate (MkByteBlock p l) = do
  p' <- app $ primIO $ primIO $ prim__duplicate_block p (cast l)
  pure1 $ (MkByteBlock p l) # (MkByteBlock p' l)

-- this copies an uncontrolled pointed-at byte block into a new linear ByteBlock
export
copy_into_bb : HasErr AppHasIO es => AnyPtr -> Int -> App1 es ByteBlock
copy_into_bb p l = do
  p' <- app $ primIO $ primIO $ prim__duplicate_block p (cast l)
  pure1 $ MkByteBlock p' (cast l)
