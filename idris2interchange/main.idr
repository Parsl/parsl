
||| Output a log message
|||
||| This is just a print right now...
log : String -> IO ()
log msg = putStrLn msg


||| This will name the C functions to use from the libzmq<->idris2
||| glue functions.
libzmq: String -> String
libzmq fn = "C:" ++ fn ++ ",glue_zmq"

||| Bind to libzmq to get a ZMQ Context
%foreign (libzmq "glue_zmq_ctx_new")
prim__zmq_ctx_new : PrimIO AnyPtr

data ZMQContext = MkZMQContext AnyPtr


new_zmq_context : IO ZMQContext
new_zmq_context = do
  ptr <- primIO $ prim__zmq_ctx_new
  -- TODO: validate ptr is not NULL, at least?
  -- and either make this return a Maybe, or
  -- terminate, or some other exception style?
  pure (MkZMQContext ptr)


main : IO ()
main = do
  log "Idris2 interchange starting"

  -- could use some with-style notation? zmq context doesn't need any cleanup
  -- if the process will just be terminated, and I think it doesn't have much
  -- in the way of linearity constraints? (although probably the sockets do?)
  zmq_ctx <- new_zmq_context

  log "Idris2 interchange ending"
