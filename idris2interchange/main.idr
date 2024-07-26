-- wow it took a long time before importing a library module...
-- the source code was 290 lines long before adding the first
-- import...
import Data.Vect
import Generics.Derive
import System.FFI

%language ElabReflection

-- for benc dev environment:
-- apt install chezscheme
-- (build idris2 if necessary)
-- export PATH=~/.idris2/bin:$PATH
-- pytest -s parsl/tests/ --config parsl/tests/configs/htex_idris2.py 



||| Output a log message
|||
||| This is just a print right now...
||| What would it look like if I was trying to do macro-style eliding
||| of log calls entirely? something involving the elaborator?
log : String -> IO ()
log msg = putStrLn msg




-- bits for interfacing to linux poll

-- TODO: can we do something to protect the lifetime of this int,
-- eg to treat it linearly (or linearly with dup-ing?) so that we
-- don't end up with bad lifetimes?
data FD = MkFD Int


-- cast is deliberatly one-way: we can easily remove the FD-ness of
-- the value, but not easily/accidentally add it onto a particular
-- arbitrary integer without declaring it using MkFD.
-- This is syntactically the same, but at a human level is a bit more
-- serious looking than cast - going along with the concept that you
-- can always cast an FD to an int meaningfully, but not the other way
-- round.
Cast FD Int where
  cast (MkFD fd) = fd

-- we need to derive Generic and Meta here, before Show will also
-- be derivable
%runElab derive "FD" [Generic, Meta, Show]

-- this is what poll takes: but revents is an output
-- so model it as input and output types that are internally
-- converted to/from what happens to be a single pollfd struct...
--       struct pollfd {
--           int   fd;         /* file descriptor */
--           short events;     /* requested events */
--           short revents;    /* returned events */
--       };

record PollInput where
  constructor MkPollInput
  fd: FD

  -- hoping that Bits16 is the same size as a C short...
  -- that's not so defined in general
  events: Bits16

record PollOutput where
  constructor MkPollOutput
  fd: FD

  revents: Bits16   -- see events short note above


-- time in milliseconds - are there other ways to represent
-- time units in idris? using a unit-forced type not int pushes
-- against some common coding mistakes I've encountered in Parsl
-- (in Parsl proper, as recommended by Kevin, a different approach
-- is to ensure the *name* of all parameters and variables contains
-- the unit - with less automatic type checking there)
data TimeMS = MkTimeMS Int

-- poll looks like this:
--        int poll(struct pollfd *fds, nfds_t nfds, int timeout);

-- in addition to being the same size, every entry in the input
-- corresponds to the entry in the output
-- (aka the FD key sequence of the input and the output is the
-- same...)
-- is there some better structure for that malloc than hoping
-- we did a free at the end? `with` style bracket or something
-- linear?

-- n is the number of (struct pollfd) in this memory allocation
-- so that we can have statically checked bounds checks (even
-- when calling into C code, as long as the interface is written
-- right)
data PollMemPtr : (n: Nat) -> Type where
  MkPollMemPtr : AnyPtr -> PollMemPtr n

%foreign "C:pollhelper_allocate_memory,pollhelper"
prim_pollhelper_allocate_memory: Int -> PrimIO AnyPtr

pollhelper_allocate_memory: (n: Nat) -> IO (PollMemPtr n)
pollhelper_allocate_memory n = do
  ptr <- primIO $ prim_pollhelper_allocate_memory (cast n)
  pure (MkPollMemPtr ptr)

pollhelper_free_memory : PollMemPtr n -> IO ()
pollhelper_free_memory (MkPollMemPtr ptr) = free ptr


%foreign "C:pollhelper_set_entry,pollhelper"
prim_pollhelper_set_entry : AnyPtr -> Int -> Int -> PrimIO ()

pollhelper_set_entry : PollMemPtr n -> Fin n -> PollInput -> IO ()
pollhelper_set_entry (MkPollMemPtr ptr) pos pi = do
  -- the cast for pos from Fin n to Int is unchecked and will break
  -- at runtime if the number is too big for Int... not compile time
  -- checked... more specifically it's the Integer to Int cast that
  -- breaks, by returning the wrong number (probably a binary bitwise
  -- least-significant-bits thing?)
  primIO $ prim_pollhelper_set_entry ptr (cast (the Integer (cast pos))) (cast pi.fd) -- TODO: flags, if I want anything other than hardcoded POLLIN

%foreign "C:pollhelper_get_entry,pollhelper"
prim_pollhelper_get_entry : AnyPtr -> Int -> PrimIO Bits16

pollhelper_get_entry : PollMemPtr n -> Fin n -> IO Bits16
pollhelper_get_entry (MkPollMemPtr ptr) pos =
  primIO $ prim_pollhelper_get_entry ptr (cast (the Integer (cast pos)))

%foreign "C:poll,libc"
prim_poll : AnyPtr -> Int -> Int -> PrimIO Int

pollhelper_poll : {n : Nat} -> PollMemPtr n -> TimeMS -> IO Int
pollhelper_poll (MkPollMemPtr ptr) (MkTimeMS t) =
  primIO $ prim_poll ptr (cast n) t

poll: {n: Nat} -> Vect n PollInput -> TimeMS -> IO (Vect n PollOutput)
poll inputs timeout = do
   -- we can't do this alloc using idris2 memory alloc because
   -- we don't have a sizeof operator or calloc (or equiv)
   buf <- pollhelper_allocate_memory n

   -- this for needs to be index by n so that we can claim that
   -- we're updating the right place
   for_ (Data.Fin.List.allFins n) $ \i => do
     -- in here, we know that i is in the range of n
     -- and so then can be used safely to index inputs
     -- as used in print statement below
     putStrLn "---"
     putStrLn "AllFins member: "
     printLn i
     putStrLn "FD at this pos:"
     -- ... here index will fail at compile time if it cannot statically
     -- verify that i is in range for inputs - there's no notion of a
     -- runtime out of range error for this index call.
     -- we aren't verifying in the type system that it is the *correct*
     -- n that we intended - that still happens by human reasoning.
     -- (also theres no '.fd is an unknown attribute' runtime error...
     -- that's also a compile time error)
     printLn (index i inputs).fd
     -- it also doesn't check we are passing in the right memory block
     -- to pollhelper_set_entry that happens to have the same count/size
     pollhelper_set_entry buf i (index i inputs)

   -- the above "allocate and set values later" looks quite like the
   -- linear immutable hole filling stuff talked about by Arnauld at tweag,
   -- although the other (.revents) part of this struct *is* mutable...

   putStrLn "About to call poll"
   poll_ret <- pollhelper_poll buf timeout -- TODO: something with the return result
   putStrLn "Poll return this return value:"
   printLn poll_ret

   -- contrast Data.Vect.alLFins here with Data.Fin.List.allFins above..
   -- we could use Data.Vect.allFins in both places I think... the reason
   -- for Data.Vect here is so the output is the desired Data.Vect too...
   r <- for (Data.Vect.allFins n) $ \i => do
        putStrLn "Extracting result for poll index"
        let inp = index i inputs
        printLn i
        printLn inp.fd
        revents <- pollhelper_get_entry buf i
        putStr "revents = "
        printLn revents
        pure (MkPollOutput inp.fd revents)

   pollhelper_free_memory buf
   pure r



||| This will name the C functions to use from the libzmq<->idris2
||| glue functions.
gluezmq: String -> String
gluezmq fn = "C:" ++ fn ++ ",glue_zmq"

||| Bind to libzmq to get a ZMQ Context
%foreign (gluezmq "glue_zmq_ctx_new")
prim__zmq_ctx_new : PrimIO AnyPtr

data ZMQContext = MkZMQContext AnyPtr

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
data ZMQSocketType = ZMQSocketDEALER | ZMQSocketREP

||| the equivalent of the #defines for socket types in /usr/include/zmq.h
zmq_socket_to_int : ZMQSocketType -> Int
zmq_socket_to_int ZMQSocketREP = 4
zmq_socket_to_int ZMQSocketDEALER = 5

-- TODO: all these AnyPtrs could be made tighter perhaps - they're all
-- pointers to specific kinds of data structure (as evidenced by their
-- immediate wrapping in type-specific wrappers...)

data ZMQSocket = MkZMQSocket AnyPtr

%foreign (gluezmq "glue_zmq_socket")
prim__zmq_socket : AnyPtr -> Int -> PrimIO AnyPtr

new_zmq_socket : ZMQContext -> ZMQSocketType -> IO ZMQSocket
new_zmq_socket (MkZMQContext ctx_ptr) socket_type = do
  ptr <- primIO (prim__zmq_socket ctx_ptr (zmq_socket_to_int socket_type))
  pure (MkZMQSocket ptr)

%foreign (gluezmq "glue_zmq_connect")
prim__zmq_connect : AnyPtr -> String -> PrimIO ()

zmq_connect : ZMQSocket -> String -> IO ()
zmq_connect (MkZMQSocket sock_ptr) dest = 
  primIO $ prim__zmq_connect sock_ptr dest


data ZMQMsg = MkZMQMsg AnyPtr

-- void *glue_zmq_recv_msg_alloc(void *sock) {
%foreign (gluezmq "glue_zmq_recv_msg_alloc")
prim__zmq_recv_msg_alloc : AnyPtr -> PrimIO AnyPtr

zmq_recv_msg_alloc : ZMQSocket -> IO ZMQMsg
zmq_recv_msg_alloc (MkZMQSocket sock_ptr) = do
    msg_ptr <- primIO $ prim__zmq_recv_msg_alloc sock_ptr
    pure (MkZMQMsg msg_ptr)


%foreign (gluezmq "glue_zmq_msg_size")
prim__zmq_msg_size : AnyPtr -> PrimIO Int

zmq_msg_size : ZMQMsg -> IO Int
zmq_msg_size (MkZMQMsg msg_ptr) = primIO $ prim__zmq_msg_size msg_ptr


%foreign (gluezmq "glue_zmq_get_socket_fd")
prim__zmq_get_socket_fd : AnyPtr -> PrimIO Int

zmq_get_socket_fd : ZMQSocket -> IO FD
zmq_get_socket_fd (MkZMQSocket sock_ptr) = do
  log "calling get_socket_fd"
  fd <- (primIO $ prim__zmq_get_socket_fd sock_ptr)
  log "retrieved fd"
  printLn fd
  pure $ MkFD fd


main : IO ()
main = do
  log "Idris2 interchange starting"

  -- could use some with-style notation? zmq context doesn't need any cleanup
  -- if the process will just be terminated, and I think it doesn't have much
  -- in the way of linearity constraints? (although probably the sockets do?)
  zmq_ctx <- new_zmq_context

  -- now we need a socket, tasks_submit_to_interchange, and once that is
  -- connected, we should see task(s) start to arrive on it without doing
  -- anything more. Maybe theres some interesting type-based sequencing for
  -- going unconnected->connected in the socket type?
  -- https://zeromq.org/socket-api/ says:
  --    ZeroMQ sockets have a life in four parts, just like BSD sockets:
  -- and some of that might be something to expose?
  -- Theres also some possibility of declaring what we're going expect over
  -- the socket (in each direction, becuase the types are different) - maybe
  -- in the socket type? which would interface with the
  -- serialiser/deserializer code?

  log "Creating tasks submit->interchange socket"
  tasks_submit_to_interchange_socket <- new_zmq_socket zmq_ctx ZMQSocketDEALER
  -- TODO: no options set here in rusterchange, though Python version might
  -- set HWM?
  -- TODO: maybe want to do some linear typing on?
  -- TODO: configuration for these ports, by reading in the configuration
  -- over stdin, as introduced in #3463?
  -- TODO: represent this unconnected state in the types from ZMQ, with
  -- whatever ZMQ's requirements are for connecting? or at least as much
  -- of those requirements as we need for the interchange...
  zmq_connect tasks_submit_to_interchange_socket "tcp://127.0.0.1:9000"
  log "Connected interchange tasks submit->interchange channel"

  -- TODO: in req/rep socket pair, the rep socket can be in two states:
  -- it's ready to recv a req, or it's received a req and is ready to send
  -- a reply. Can I encoded that state into the command channel so that
  -- it alternates? The goal would be to prove at a type level that
  -- every req gets a rep, or something like that...
  -- in Parsl submit-side, for example, what happens when things break?
  -- there's this ugly:
  --
  --            except zmq.ZMQError:
  --                logger.exception("Potential ZMQ REQ-REP deadlock caught")
  --                logger.info("Trying to reestablish context")
  --                self.zmq_context.recreate()
  --                self.create_socket_and_bind()
  -- ... and I have no particular believe that that is resilient (having
  -- never consciously seen it fire...) - that feels to me like a
  -- prototype-era "saw weird exception, put in a hack"

  log "Creating interchange command channel socket"
  command_socket <- new_zmq_socket zmq_ctx ZMQSocketREP
  -- TODO: represent unconnected state here?
  zmq_connect command_socket "tcp://127.0.0.1:9002"
  log "Connected interchange command channel"

  -- TODO: probably should create result socket here

  -- TODO: polling of some kind here to decide which socket we're going to
  -- ask for a message (or other kind of event that might happen - eg.
  -- the heartbeat timeouts or system shutdown signals?)
  -- Right now this code will use ZMQ polling, but I think it would be fine
  -- to use fd based polling like the elixirchange does, too...
  -- tempting to use some kind of co-routine behaviour to drive things,
  -- but from rusterchange experience, there maybe isn't enough complexity
  -- to move away from looping and processing each message - for the idris2
  -- implementation, choices there should be driven by what makes things
  -- easiest and/or most novel in terms of types...
  -- we've got command_socket and tasks_submit_to_interchange socket to poll on...
  -- let's do it fd driven right from the start... might be fun to try some
  -- special fd types etc... it can be linux specific and that's ok in this
  -- prototyping context... signalfd for exits, timerfd for timing? epoll for
  -- gratuitous complication? this bit is experimenting with modern linux
  -- file descriptor behaviour...

  -- does a file descriptor need any additions to a basic FileDescriptor type?
  -- is there anything to statically permit/prohibit etc?
  -- in the rusterchange i was a bit concerned
  -- about the relationship between zmq_poller results and what was done with
  -- those polled results...
  log "poll()ing for activity"

  -- poll looks like this:
  --        int poll(struct pollfd *fds, nfds_t nfds, int timeout);
  -- with pollfd like this:
  --       struct pollfd {
  --           int   fd;         /* file descriptor */
  --           short events;     /* requested events */
  --           short revents;    /* returned events */
  --       };

  -- these pollfd structures are both an input and an output... .events
  -- is an input and .revents is a return, so I guess I want to model this
  -- API in something that looks more like a function... (eg take an fd
  -- and events list, and return a list of fd, revents?)

  -- we will poll:
  --  tasks_submit_to_interchange_socket
  --  command_socket

  -- so first we need to ask those two sockets for their FDs
  tasks_submit_to_interchange_fd <- zmq_get_socket_fd tasks_submit_to_interchange_socket
  command_fd <- zmq_get_socket_fd command_socket

  poll_outputs <- poll [MkPollInput command_fd 0,
                        MkPollInput tasks_submit_to_interchange_fd 0]
                       (MkTimeMS 4000)   -- should either be infinity or managed as part of a timed events queue that is not fds?

  log "poll completed"

  -- Semantics of buffer ownership: the caller must allocate a buffer and
  -- describe the size to zmq_recv. Probably some linear type stuff to be
  -- done here to ensure the buffer gets used properly? (maybe even type
  -- state for: not populated, populated by a message which can be read
  -- many times -> delete/reuse? or perhaps something region based to let
  -- the buffer be usable inside the region but not outside it?)
  -- TODO: biggest danger is making sure it is freed safely/appropriately...
  -- (after all uses, and eventually - pretty much 1-multiplicity semantics?)
  -- imagine an API: recvAndThen which takes a linear continuation?

  log "Waiting to receive a message from task submit->interchange channel"
  msg <- zmq_recv_msg_alloc tasks_submit_to_interchange_socket

  putStr "Received message, size "
  s <- zmq_msg_size msg
  printLn s

  -- so now we've received a message... we'll need to eventually deallocate
  -- it... and hopefully have the type system enforce that... TODO

  -- what msg contains here is a pickle-encoded two element dictionary,
  -- the task ID and the buffer.
  -- so... now its time to write a pickle decoder?

  log "Idris2 interchange ending"
