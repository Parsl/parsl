||| Kinda like Data.Buffer but also not...
||| This represents a buffer of data that lives in an FFI memory
||| allocation, with a length. The intention is to represent the
||| message piece of a zmq_msg_t for the purposes of parsing it
||| using a pickle decoder.
module Bytes

%default total

public export
data ByteBlock = MkByteBlock AnyPtr Nat


-- i think this doesn't need to be in IO because its
-- not doing anything that allocates resources: it
-- uses a NULL pointer for the memory allocation, and
-- because it is length 0, nothing should ever
-- dereference that.
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

-- S n gives us proof that ByteBlock is not empty
export
bb_uncons : ByteBlock -> IO (Bits8, ByteBlock)
bb_uncons (MkByteBlock ptr (S n)) = do

  v <- primIO $ prim__readByteAt ptr

  let ptr_inc = incPtr ptr
  let rest = MkByteBlock ptr_inc n

  pure (v, rest)

bb_uncons (MkByteBlock _ Z) = ?error_unconsing_from_empty_byteblock


%foreign "C:copy_and_append,bytes"
prim__copy_and_append: AnyPtr -> Int -> Bits8 -> PrimIO AnyPtr

export
bb_append : ByteBlock -> Bits8 -> IO ByteBlock
bb_append (MkByteBlock ptr n) v = do
  -- can't necessarily realloc here, because ptr is not
  -- necessarily a malloced ptr: it might be a pointer
  -- further along into the block... and without any 
  -- linearity we don't have any guarantee about other
  -- ByteBlocks sharing the same underlying memory...
  -- (which is also a problem for arbitrary mutability)
  new_ptr <- primIO $ prim__copy_and_append ptr (cast n) v
  pure (MkByteBlock (new_ptr) (S n))


covering export
bb_append_bytes : ByteBlock -> ByteBlock -> IO ByteBlock
bb_append_bytes a@(MkByteBlock _ _) b@(MkByteBlock _ m) = case m of
  Z => pure a
  S x => do
    (v, rest) <- bb_uncons b
    a' <- bb_append a v
    bb_append_bytes a' rest
    

export
length : ByteBlock -> Nat
length (MkByteBlock _ l) = l


%foreign "C:str_from_bytes,bytes"
prim__str_from_bytes : Int -> AnyPtr -> PrimIO String

export
str_from_bytes : Nat -> ByteBlock -> IO (String, ByteBlock)
str_from_bytes l (MkByteBlock p l') = do
  s <- primIO $ prim__str_from_bytes (cast l) p
  let rest = MkByteBlock (incPtrBy (cast l) p) (l' `minus` l)
  pure (s, rest)

%foreign "C:unicode_byte_len,bytes"
prim__unicode_byte_len : String -> PrimIO Int

%foreign "C:unicode_bytes,bytes"
prim__unicode_bytes : String -> PrimIO AnyPtr

export
bytes_from_str : String -> IO ByteBlock
bytes_from_str s = do
  len <- primIO $ prim__unicode_byte_len s
  strbytes <- primIO $ prim__unicode_bytes s
  pure $ MkByteBlock strbytes (cast len)

