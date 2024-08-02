||| Kinda like Data.Buffer but also not...
||| This represents a buffer of data that lives in an FFI memory
||| allocation, with a length. The intention is to represent the
||| message piece of a zmq_msg_t for the purposes of parsing it
||| using a pickle decoder.
module Bytes

public export
data ByteBlock : Nat -> Type where
  MkByteBlock : AnyPtr -> (l: Nat) -> ByteBlock l
