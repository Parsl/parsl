module ZMQ

import Control.App

import Bytes
import FD
import Logging

-- Control.App isn't total in the way that IO seems to be... %default total

||| This will name the C functions to use from the libzmq<->idris2
||| glue functions.
gluezmq: String -> String
gluezmq fn = "C:" ++ fn ++ ",glue_zmq"

||| Bind to libzmq to get a ZMQ Context
%foreign (gluezmq "glue_zmq_ctx_new")
prim__zmq_ctx_new : PrimIO AnyPtr

-- this is not public so -- so that outside of this source file, no one can get at the context
-- point, and use the libzmq context.
-- 'export' means the type name is available elsewhere but the constructor is not.
export data ZMQContext = MkZMQContext AnyPtr

new_zmq_context : HasErr AppHasIO es => App es ZMQContext
new_zmq_context = do
  ptr <- primIO $ primIO $ prim__zmq_ctx_new
  -- TODO: validate ptr is not NULL, at least?
  -- and either make this return a Maybe, or
  -- terminate, or some other exception style?
  pure (MkZMQContext ptr)

-- it's possible that ZMQContext should turn into an app-level state-like
-- thing, with the value hidden so that it can't be captured, returned and
-- used after close? In which case, it woul be something like a reader
-- with ZMQContext content? and all the context-using functions changed to
-- only use the current context rather than an arbitrary one passed in.
-- so it doesn't matter if the user has managed to read the context value,
-- they can't use it because no function takes it as a paremeter?
-- (not actually true - this doesn't stop the user from making their own
-- reader context with some arbitrarily acquired context pointer? how
-- can I stop that? still tie the context to a region?)
-- closing the context will hang if sockets are still open so maybe its
-- useful to somehow insist that all sockets are closed, statically?
public export
with_zmq_context: HasErr AppHasIO es => (ZMQContext -> App es a) -> App es a
with_zmq_context body = do
  ctx <- new_zmq_context
  body ctx
  close_zmq_context ctx
  -- ^ i think its especially important to be checking error codes with this
  -- close because I'd like to see if the close is failing due to all this
  -- other stuff still being open. (and then... type level ensuring all
  -- sockets are closed before being able to exit the with-block?)

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
data ZMQSocketType = ZMQSocketDEALER | ZMQSocketREP | ZMQSocketROUTER

||| the equivalent of the #defines for socket types in /usr/include/zmq.h
zmq_socket_to_int : ZMQSocketType -> Int
zmq_socket_to_int ZMQSocketREP = 4
zmq_socket_to_int ZMQSocketDEALER = 5
zmq_socket_to_int ZMQSocketROUTER = 6

-- TODO: all these AnyPtrs could be made tighter perhaps - they're all
-- pointers to specific kinds of data structure (as evidenced by their
-- immediate wrapping in type-specific wrappers...)

public export
data ZMQSocket = MkZMQSocket AnyPtr

%foreign (gluezmq "glue_zmq_socket")
prim__zmq_socket : AnyPtr -> Int -> PrimIO AnyPtr

public export
new_zmq_socket : HasErr AppHasIO es => ZMQContext -> ZMQSocketType -> App es ZMQSocket
new_zmq_socket (MkZMQContext ctx_ptr) socket_type = do
  ptr <- primIO $ primIO (prim__zmq_socket ctx_ptr (zmq_socket_to_int socket_type))
  pure (MkZMQSocket ptr)

%foreign (gluezmq "glue_zmq_connect")
prim__zmq_connect : AnyPtr -> String -> PrimIO ()

public export
zmq_connect : HasErr AppHasIO es => ZMQSocket -> String -> App es ()
zmq_connect (MkZMQSocket sock_ptr) dest = 
  primIO $ primIO $ prim__zmq_connect sock_ptr dest

%foreign (gluezmq "glue_zmq_bind")
prim__zmq_bind : AnyPtr -> String -> PrimIO ()

public export
zmq_bind : HasErr AppHasIO es => ZMQSocket -> String -> App es ()
zmq_bind (MkZMQSocket sock_ptr) dest = 
  primIO $ primIO $ prim__zmq_bind sock_ptr dest

public export
data ZMQMsg = MkZMQMsg AnyPtr

-- void *glue_zmq_recv_msg_alloc(void *sock) {
%foreign (gluezmq "glue_zmq_recv_msg_alloc")
prim__zmq_recv_msg_alloc : AnyPtr -> PrimIO AnyPtr

public export
zmq_recv_msg_alloc : HasErr AppHasIO es => ZMQSocket -> App es (Maybe ZMQMsg)
zmq_recv_msg_alloc (MkZMQSocket sock_ptr) = do
    msg_ptr <- primIO $ primIO $ prim__zmq_recv_msg_alloc sock_ptr
    if prim__nullAnyPtr msg_ptr == 1 
      then pure $ Nothing
      else pure $ Just $ MkZMQMsg msg_ptr

%foreign "C:zmq_msg_close,libzmq"
prim__zmq_msg_close : AnyPtr -> PrimIO ()

%foreign "C:free,libc"
prim__free : AnyPtr -> PrimIO ()

public export
zmq_msg_close : HasErr AppHasIO es => ZMQMsg -> App es ()
zmq_msg_close (MkZMQMsg msg_ptr) = do
  primIO $ primIO $ prim__zmq_msg_close msg_ptr
  primIO $ primIO $ prim__free msg_ptr


%foreign (gluezmq "glue_zmq_msg_more")
prim__zmq_msg_more : AnyPtr -> PrimIO Int

public export
zmq_msg_more : HasErr AppHasIO es => ZMQMsg -> App es Bool
zmq_msg_more (MkZMQMsg msg_ptr) = do
  flag <- primIO $ primIO $ prim__zmq_msg_more msg_ptr
  case flag of
    0 => pure False
    1 => pure True
    _ => ?error_out_of_range_zmq_msg_more

public export
zmq_recv_msgs_multipart_alloc : HasErr AppHasIO es => ZMQSocket -> App es (List ZMQMsg)
zmq_recv_msgs_multipart_alloc socket = do
  -- structure could be something like:
  -- recursively, keep invoking get msg while the 'more' flag is set
  maybe_head_msg <- zmq_recv_msg_alloc socket
  case maybe_head_msg of 
    Nothing => pure []
    Just head_msg => do
      more <- zmq_msg_more head_msg
      if more
        then do
          rest_msgs <- zmq_recv_msgs_multipart_alloc socket
          pure (head_msg :: rest_msgs)
        else pure [head_msg]

%foreign (gluezmq "glue_zmq_msg_size")
prim__zmq_msg_size : AnyPtr -> PrimIO Int

public export
zmq_msg_size : HasErr AppHasIO es => ZMQMsg -> App {l} es Int
zmq_msg_size (MkZMQMsg msg_ptr) = primIO $ primIO $ prim__zmq_msg_size msg_ptr

%foreign (gluezmq "glue_zmq_msg_data")
prim__zmq_msg_data : AnyPtr -> PrimIO AnyPtr

zmq_msg_data : HasErr AppHasIO es => ZMQMsg -> App {l} es AnyPtr
zmq_msg_data (MkZMQMsg msg_ptr) = primIO $ primIO $ prim__zmq_msg_data msg_ptr

export
zmq_msg_as_bytes : HasErr AppHasIO es => ZMQMsg -> App1 es ByteBlock
zmq_msg_as_bytes msg = do
  size <- app $ zmq_msg_size msg
  byte_ptr <- (app $ zmq_msg_data msg)
  -- we get a pointer here... but it is not managed by malloc, so it's not
  -- safe to represent it with a ByteBlock. The memory release semantics are
  -- (if i understand ZMQ) that the memory will be release when the ZMQMsg is
  -- closed, rather than the pointer itself being released by the user. Maybe
  -- there's some type annotations on the pointer that can be used to indicate
  -- that? Right now it's probably easier to make a copy of the data into a
  -- malloc area.
  copy_into_bb byte_ptr size

public export data ZMQFD : Type where

public export Pollable ZMQFD where

-- public export Pollable ZMQFD
-- the above line gives me a *runtime* error!
-- Encountered undefined name ZMQ.Pollable implementation at ZMQ:166:1--166:29
-- which vibes like a compiler bug to me? I was getting a FD.Pollable is not visible error before making FD.Pollable into public export; so at compile time it's finding the right name at least; and renaming this to XPollable successfully says XPollable doesn't exist; and removing the line successfully gives me compile time type erorr that ZMQFD cannot be used with MkPollInput.
-- This is with idris2 Version 0.7.0-f7c6b1990 + my own patch for non-monadic bind in Control/App.idr >>
-- so let's try the latest idris2 to see if there's a bug thats been fixed, at least?
-- In the latest still broken with same error.
-- Looking at the scheme source: for PidFD and ZMQFD, the error message is a bit misleadingly emitted like this,
-- by what i guess is meant to actually contain the implementation?
-- (define (FD-u--__Impl_Pollable_PidFD . any-args) (blodwen-error-quit "Encountered undefined name FD.Pollable implementation at FD:45:1--45:29"))
-- (define (ZMQ-u--__Impl_Pollable_ZMQFD . any-args) (blodwen-error-quit "Encountered undefined name ZMQ.Pollable implementation at ZMQ:166:1--166:29"))
-- Turns out adding `where` onto the end seems to fix it: not sure (and maybe an idris2 question/issue) what it means to not have the `where` on there? seems to declare it exists without declaring a value for it? But maybe i'd prefer an error/warning?

%foreign (gluezmq "glue_zmq_get_socket_fd")
prim__zmq_get_socket_fd : AnyPtr -> PrimIO Int

public export
zmq_get_socket_fd : (State LogConfig LogConfig es, HasErr AppHasIO es) => ZMQSocket -> App es (FD ZMQFD)
zmq_get_socket_fd (MkZMQSocket sock_ptr) = do
  log "calling get_socket_fd"
  fd <- (primIO $ primIO $ prim__zmq_get_socket_fd sock_ptr)
  logv "retrieved fd" fd
  pure $ MkFD fd

%foreign (gluezmq "glue_zmq_get_socket_events")
prim__zmq_get_socket_events : AnyPtr -> PrimIO Int

public export
zmq_get_socket_events : (State LogConfig LogConfig es, HasErr AppHasIO es) => ZMQSocket -> App es Int
zmq_get_socket_events (MkZMQSocket sock_ptr) = do
  log "calling get_socket_events"
  events <- primIO $ primIO $ prim__zmq_get_socket_events sock_ptr
  logv "retrieved these event flags" events
  pure events


%foreign (gluezmq "glue_zmq_send_bytes")
prim__zmq_send_bytes : AnyPtr -> AnyPtr -> Int -> Int -> PrimIO ()

public export
zmq_send_bytes : (State LogConfig LogConfig es, HasErr AppHasIO es) => ZMQSocket -> ByteBlock -> Bool -> App {l} es ()
zmq_send_bytes (MkZMQSocket sock_ptr) (MkByteBlock byte_ptr size) more = Control.App.do
  log "sending bytes"
  let more_i = if more then 1 else 0
  primIO $ primIO $ prim__zmq_send_bytes sock_ptr byte_ptr (cast size) more_i
  log "sent bytes"

public export
zmq_send_bytes1 : (State LogConfig LogConfig es, HasErr AppHasIO es) => ZMQSocket -> (1 _ : ByteBlock) -> Bool -> App1 es ByteBlock
zmq_send_bytes1 (MkZMQSocket sock_ptr) (MkByteBlock byte_ptr size) more = do
  log "sending bytes"
  let more_i = if more then 1 else 0
  app $ primIO $ primIO $ prim__zmq_send_bytes sock_ptr byte_ptr (cast size) more_i
  log "sent bytes"
  pure1 (MkByteBlock byte_ptr size)
