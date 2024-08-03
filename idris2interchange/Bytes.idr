||| Kinda like Data.Buffer but also not...
||| This represents a buffer of data that lives in an FFI memory
||| allocation, with a length. The intention is to represent the
||| message piece of a zmq_msg_t for the purposes of parsing it
||| using a pickle decoder.
module Bytes

public export
data ByteBlock : Nat -> Type where
  MkByteBlock : AnyPtr -> (l: Nat) -> ByteBlock l

%foreign "C:readByteAt,bytes"
prim__readByteAt : AnyPtr -> PrimIO Bits8

readByteAt : AnyPtr -> IO Bits8
readByteAt p = primIO $ prim__readByteAt p

%foreign "C:incPtr,bytes"
prim__incPtr : AnyPtr -> AnyPtr

incPtr : AnyPtr -> AnyPtr
incPtr p = prim__incPtr p

-- S n gives us proof that ByteBlock is not empty
export
bb_uncons : ByteBlock (S n) -> IO (Bits8, ByteBlock n)
bb_uncons (MkByteBlock ptr (S n)) = do
  v <- readByteAt ptr
  let ptr_inc = incPtr ptr
  let rest = MkByteBlock ptr_inc n

  pure (v, rest)


