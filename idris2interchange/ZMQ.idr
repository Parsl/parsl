module ZMQ

import Bytes
import FD
import Logging

||| This will name the C functions to use from the libzmq<->idris2
||| glue functions.
gluezmq: String -> String
gluezmq fn = "C:" ++ fn ++ ",glue_zmq"

||| Bind to libzmq to get a ZMQ Context
%foreign (gluezmq "glue_zmq_ctx_new")
prim__zmq_ctx_new : PrimIO AnyPtr

data ZMQContext = MkZMQContext AnyPtr

public export
new_zmq_context : IO ZMQContext
new_zmq_context = do
  ptr <- primIO $ prim__zmq_ctx_new
  -- TODO: validate ptr is not NULL, at least?
  -- and either make this return a Maybe, or
  -- terminate, or some other exception style?
  pure (MkZMQContext ptr)


-- a partial manual enumeration of socket types
-- (only enough for making this interchange work)
-- TODO: can these get namespaced more like Zmq.Socket and
-- Zmq.Socket.DEALER?
-- can I find notation like Python's IntEnum where the
-- equivalent C number is written close to the constructor name?
-- Can I import them more directly from zmq.h? (involving the
-- horror that others have described of how awful it is to properly
-- parse C code?)
public export
data ZMQSocketType = ZMQSocketDEALER | ZMQSocketREP

||| the equivalent of the #defines for socket types in /usr/include/zmq.h
zmq_socket_to_int : ZMQSocketType -> Int
zmq_socket_to_int ZMQSocketREP = 4
zmq_socket_to_int ZMQSocketDEALER = 5

-- TODO: all these AnyPtrs could be made tighter perhaps - they're all
-- pointers to specific kinds of data structure (as evidenced by their
-- immediate wrapping in type-specific wrappers...)

public export
data ZMQSocket = MkZMQSocket AnyPtr

%foreign (gluezmq "glue_zmq_socket")
prim__zmq_socket : AnyPtr -> Int -> PrimIO AnyPtr

public export
new_zmq_socket : ZMQContext -> ZMQSocketType -> IO ZMQSocket
new_zmq_socket (MkZMQContext ctx_ptr) socket_type = do
  ptr <- primIO (prim__zmq_socket ctx_ptr (zmq_socket_to_int socket_type))
  pure (MkZMQSocket ptr)

%foreign (gluezmq "glue_zmq_connect")
prim__zmq_connect : AnyPtr -> String -> PrimIO ()

public export
zmq_connect : ZMQSocket -> String -> IO ()
zmq_connect (MkZMQSocket sock_ptr) dest = 
  primIO $ prim__zmq_connect sock_ptr dest


data ZMQMsg = MkZMQMsg AnyPtr

-- void *glue_zmq_recv_msg_alloc(void *sock) {
%foreign (gluezmq "glue_zmq_recv_msg_alloc")
prim__zmq_recv_msg_alloc : AnyPtr -> PrimIO AnyPtr

public export
zmq_recv_msg_alloc : ZMQSocket -> IO (Maybe ZMQMsg)
zmq_recv_msg_alloc (MkZMQSocket sock_ptr) = do
    msg_ptr <- primIO $ prim__zmq_recv_msg_alloc sock_ptr
    if prim__nullAnyPtr msg_ptr == 1 
      then pure $ Nothing
      else pure $ Just $ MkZMQMsg msg_ptr


%foreign (gluezmq "glue_zmq_msg_size")
prim__zmq_msg_size : AnyPtr -> PrimIO Int

public export
zmq_msg_size : ZMQMsg -> IO Int
zmq_msg_size (MkZMQMsg msg_ptr) = primIO $ prim__zmq_msg_size msg_ptr

%foreign (gluezmq "glue_zmq_msg_data")
prim__zmq_msg_data : AnyPtr -> PrimIO AnyPtr

zmq_msg_data : ZMQMsg -> IO AnyPtr
zmq_msg_data (MkZMQMsg msg_ptr) = primIO $ prim__zmq_msg_data msg_ptr

export
zmq_msg_as_bytes : ZMQMsg -> IO (n: Nat ** (ByteBlock n))
zmq_msg_as_bytes msg = do
  size <- cast <$> zmq_msg_size msg
  byte_ptr <- zmq_msg_data msg
  pure (size ** (MkByteBlock byte_ptr size))

%foreign (gluezmq "glue_zmq_get_socket_fd")
prim__zmq_get_socket_fd : AnyPtr -> PrimIO Int

public export
zmq_get_socket_fd : ZMQSocket -> IO FD
zmq_get_socket_fd (MkZMQSocket sock_ptr) = do
  log "calling get_socket_fd"
  fd <- (primIO $ prim__zmq_get_socket_fd sock_ptr)
  log "retrieved fd"
  printLn fd
  pure $ MkFD fd

