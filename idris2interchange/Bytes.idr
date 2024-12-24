||| Kinda like Data.Buffer but also not...
||| This represents a buffer of data that lives in an FFI memory
||| allocation, with a length. The intention is to represent the
||| message piece of a zmq_msg_t for the purposes of parsing it
||| using a pickle decoder.
module Bytes

%default total

public export
data ByteBlock : Nat -> Type where
  MkByteBlock : AnyPtr -> (l: Nat) -> ByteBlock l


-- i think this doesn't need to be in IO because its
-- not doing anything that allocates resources: it
-- uses a NULL pointer, and the length 0 means that
-- nothing will ever try to dereference that pointer,
-- although need to have caution on any kind of
-- realloc/free... 
export
emptyByteBlock : ByteBlock 0
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
bb_uncons : ByteBlock (S n) -> IO (Bits8, ByteBlock n)
bb_uncons (MkByteBlock ptr (S n)) = do

  v <- primIO $ prim__readByteAt ptr

  let ptr_inc = incPtr ptr
  let rest = MkByteBlock ptr_inc n

  pure (v, rest)

%foreign "C:copy_and_append,bytes"
prim__copy_and_append: AnyPtr -> Int -> Bits8 -> PrimIO AnyPtr

export
bb_append : ByteBlock n -> Bits8 -> IO (ByteBlock (S n))
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
bb_append_bytes : ByteBlock n -> (m ** ByteBlock m) -> IO (nm ** ByteBlock nm)
bb_append_bytes a@(MkByteBlock _ al) (m ** b) = case m of
  Z => pure (al ** a)
  S x => do
    (v, rest) <- bb_uncons b
    a' <- bb_append a v
    bb_append_bytes a' (x ** rest)
    

export
length : ByteBlock n -> Nat
length (MkByteBlock _ l) = l


%foreign "C:str_from_bytes,bytes"
prim__str_from_bytes : Int -> AnyPtr -> PrimIO String

export
str_from_bytes : (m : Nat) -> ByteBlock n -> IO (String, ByteBlock (minus n m))
str_from_bytes l (MkByteBlock p l') = do
  s <- primIO $ prim__str_from_bytes (cast l) p
  let rest = MkByteBlock (incPtrBy (cast l) p) (l' `minus` l)
  pure (s, rest)

%foreign "C:unicode_byte_len,bytes"
prim__unicode_byte_len : String -> PrimIO Int

%foreign "C:unicode_bytes,bytes"
prim__unicode_bytes : String -> PrimIO AnyPtr

export
bytes_from_str : String -> IO (n : Nat ** ByteBlock n)
bytes_from_str s = do
  len <- primIO $ prim__unicode_byte_len s
  strbytes <- primIO $ prim__unicode_bytes s
  pure (cast len ** MkByteBlock strbytes (cast len))

