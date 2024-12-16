module Main

-- wow it took a long time before importing a library module...
-- the source code was 290 lines long before adding the first
-- import...
import Control.App
import Data.Vect
import Generics.Derive
import Language.JSON
import System.FFI

import Bytes
import FD
import Logging
import Pickle
import ZMQ

%language ElabReflection
-- %default total


-- TODO: this should eventually be chosen according to
-- config supplied on stdin, or by random bind.
WORKER_TASK_PORT : Int
WORKER_TASK_PORT = 9003

WORKER_RESULT_PORT : Int
WORKER_RESULT_PORT = 9004

-- for benc dev environment:
-- apt install chezscheme      -- to run idris
-- apt install libzmq3-dev     -- for my own zmq bindings
-- (build idris2 if necessary)
-- export PATH=~/.idris2/bin:$PATH
-- pytest -s parsl/tests/ --config parsl/tests/configs/htex_idris2.py 



record MatchState where
  constructor MkMatchState
  managers: List JSON
  tasks: List PickleAST
%runElab derive "MatchState" [Generic, Meta, Show]



-- this should be total, proved by decreasing n
-- but apparently not? cheat by using assert_smaller...
inner_ascii_dump : HasErr AppHasIO es => (n : Nat ** ByteBlock n) -> App es ()
inner_ascii_dump (Z ** _) = pure ()
inner_ascii_dump i@(S n' ** bytes) = do
  (b, rest) <- primIO $ bb_uncons bytes
  if b >= 32 && b < 128
    then primIO $ putStr (singleton (chr $ cast b))
    else primIO $ putStr "."
  inner_ascii_dump (assert_smaller i (n' ** rest))

ascii_dump : HasErr AppHasIO es => (n : Nat ** ByteBlock n) -> App es ()
ascii_dump v = do
  inner_ascii_dump v
  primIO $ putStrLn ""


-- the Python type contained in the returned AST depends on the supplied
-- command... TODO: maybe I can describe that typing in the idris2 code
-- rather than returning an equivalent to Python Any... so that the
-- individual dispatch_cmd pieces can be typechecked a bit?
dispatch_cmd : HasErr AppHasIO es => String -> App es PickleAST

dispatch_cmd "WORKER_PORTS" = do
  log "WORKER_PORTS requested"
  -- hard-code return value because ports are also hard-coded ...
  -- TODO: this should be some environment to be passed around
  -- perhaps as a monad-style reader environment?
  pure (PickleTuple [PickleInteger WORKER_TASK_PORT, PickleInteger WORKER_RESULT_PORT])

dispatch_cmd "CONNECTED_BLOCKS" = do
  log "CONNECTED_BLOCKS requested"
  -- TODO: in the rusterchange, it seemed like connected blocks didn't
  -- need implementing for the test suite, and could return an empty
  -- list. So that's what I'll do here.
  pure (PickleList [])

dispatch_cmd _ = ?error_cmd_not_implemented


matchmake :  (State MatchState MatchState es, HasErr AppHasIO es) => App es ()
matchmake = do
  log "Matchmaker starting"
  -- we can make a match if we have a manager with capacity and a task in the
  -- task queue. This is the point that in the real htex interchange can do
  -- more interesting matching.

  -- selecting a task is easy: it's any task from the task list, and so the
  -- head of the task list will do, and it's cheap to extract and leave the
  -- remaining tasks as a whole list.

  -- selecting a manager is harder: the state here holds all the managers
  -- with a slot count. we need any manager with a non-zero slot count.
  -- maybe there's a more efficient data structure here that can deal with
  -- slotcount == 0, /= 0 being the primary selection mechanism being the
  -- primary selection mechanism. eg. some functional structure that can
  -- be ordered by slotcount. or split into two lists: the 0s and the non-0s.
  -- (similar to the interesting_managers vs ready_managers distinction in
  -- real interchange, which was implemented to avoid linear scan of all
  -- busy managers)

  -- transactionally: get a task; get a manager; if we succeed at both,
  -- dispatch task and update task (by removal) and manager (by decrement)
  -- lists in MatchState.

  (MkMatchState managers tasks) <- get MatchState

  case tasks of
    [] => log "No tasks in matchmaker - no match possible"
    task :: rest_tasks => do
      logv "This task is available for matchmaking" task
      -- manager selection more complicated, especially with replacement/removal
      -- of manager value with a decremented value.
      -- perhaps a "take and subtract" function which returns both a manager
      -- object but also the full modified manager list? or the manager list
      -- without the selected entry, with the intention of adding it back in
      -- later? Will also need to be able to increments on this structure when
      -- task results come in.
      -- In the rust impl, I represented managers using multiple slot objects,
      -- as many as listed in the original count.
      case managers of
        [] => log "No managers registered - no match possible"
        m :: rest_managers => do
          logv "Considering manager for match" m
          let c = lookup "max_capacity" m
          case c of
            Just (JNumber c) => do
              logv "This manager has capacity" c
              if c > 0
                then ?notimpl_matchmake
                else ?notimpl_manager_oversubscribed
            _ => ?error_registration_bad_max_capacity
  log "Matchmaker (no-op) completed"


||| this drains ZMQ when there's a poll. this is a bit complicated and
||| not like a regular level-triggered poll.
||| Some reading:
||| https://funcptr.net/2012/09/10/zeromq---edge-triggered-notification/
||| https://github.com/zeromq/libzmq/issues/3641
covering zmq_poll_command_channel_loop : HasErr AppHasIO es => ZMQSocket -> App es ()
zmq_poll_command_channel_loop command_socket = do
  -- need to run this in a loop until it returns no events left
  events <- zmq_get_socket_events command_socket
  logv "ZMQ poll command channel loop got these command channel events" events

  -- TODO: ignoring writeability of this channel. I think thats the right
  -- thing to be doing: being ready for a write doesn't mean we can write
  -- to it immediately. (?)

  when (events == 1) $ do  -- read, and no write. if there's a write event in here,
                           -- this won't match
    log "Trying to receive a message from command channel"
    -- TODO: something here to force the command socket to statically only
    -- be usable to send back the response to...
    maybe_msg <- zmq_recv_msg_alloc command_socket
    case maybe_msg of
      Nothing => log "No message received."
      Just msg => do
        s <- zmq_msg_size msg
        logv "Received message, size" s

        -- so now we've received a message... we'll need to eventually deallocate
        -- it... and hopefully have the type system enforce that... TODO

        -- what msg contains here is a pickle-encoded two element dictionary,
        -- the task ID and the buffer.
        -- so... now its time to write a pickle decoder?
        bytes <- zmq_msg_as_bytes msg
        (PickleUnicodeString cmd) <- unpickle bytes
            | _ => ?error_cmd_is_not_a_string
        logv "Command received this command" cmd
        resp <- dispatch_cmd cmd
        logv "Response to command" resp
        (n ** resp_bytes) <- pickle resp
        zmq_alloc_send_bytes command_socket resp_bytes
        -- need to do some appropriate de-alloc for a message here?
        -- or is it done inside alloc_send_bytes?

        -- after this send, command socket is back is ready-for-a-REQ state

        zmq_poll_command_channel_loop command_socket

-- TODO: factor ZMQ_EVENTS handling with other channels
covering zmq_poll_tasks_submit_to_interchange_loop : (State MatchState MatchState es, HasErr AppHasIO es) => ZMQSocket -> App es ()
zmq_poll_tasks_submit_to_interchange_loop tasks_submit_to_interchange_socket = do
  -- need to run this in a loop until it returns no events left
  events <- zmq_get_socket_events tasks_submit_to_interchange_socket
  logv "ZMQ poll tasks submit to interchange loop got these zmq events" events

  -- TODO: ignoring writeability of this channel. I think thats the right
  -- thing to be doing: being ready for a write doesn't mean we can write
  -- to it immediately. (?)

  when (events `mod` 2 == 1) $ do  -- read
    log "Trying to receive a message from task submit->interchange channel"
    maybe_msg <- zmq_recv_msg_alloc tasks_submit_to_interchange_socket
    case maybe_msg of
      Nothing => log "No message received on task submit->interchange channel"
      Just msg => do
        s <- zmq_msg_size msg
        logv "Received task-like message on task submit->interchange channel, size" s
        bytes <- zmq_msg_as_bytes msg
        ascii_dump bytes

        task <- unpickle bytes

        logv "Unpickled task" task

        -- TODO: add to match state

        old_state@(MkMatchState managers tasks) <- get MatchState
        logv "Old match state" old_state
        put MatchState (MkMatchState managers (task :: tasks))
        log "Put new task into match state" 
        matchmake

        -- TODO: deallocate the message and bytes (after unpickling)

        -- TODO: so now we've received a message... we'll need to eventually deallocate
        -- and also do something with the message

        zmq_poll_tasks_submit_to_interchange_loop tasks_submit_to_interchange_socket

covering zmq_poll_tasks_interchange_to_worker_loop : (State MatchState MatchState es, HasErr AppHasIO es) => ZMQSocket -> App es ()
zmq_poll_tasks_interchange_to_worker_loop tasks_interchange_to_worker_socket = do
  events <- zmq_get_socket_events tasks_interchange_to_worker_socket
  logv "ZMQ poll tasks interchange to worker loop got these zmq events" events
  -- TODO: we'll be both reading and writing from this socket

  when (events `mod` 2 == 1) $ do -- read
    log "Reading message from task interchange->worker channel"
    -- This socket is a ROUTER socket, so there will be a multi-part
    -- message where the first part is going to be the identifier of the
    -- sender. I think if one part is available, all parts will be available,
    -- because they're delivered all as one on-the-wire message. I haven't
    -- seen that be explicitly noted, though.

    -- TODO: Idris2 level API for receiving multipart messages, returning a
    -- list of msg objects, instead of a Maybe.
    msgs <- zmq_recv_msgs_multipart_alloc tasks_interchange_to_worker_socket
    case msgs of
      [] => log "No message received on task interchange->worker channel"
      [addr_part, json_part] => do
        bb@(s ** bytes) <- zmq_msg_as_bytes json_part
        logv "Received two-part message on task interchange->worker channel, size" s
        ascii_dump bb

        (msg_as_str, _) <- primIO $ str_from_bytes (cast s) bytes
        let mj = parse msg_as_str

        case mj of
          Nothing => ?error_no_json_parse_of_registration_like
          Just j => do
            logv "Parsed JSON" j

            -- TODO: is this always registration? I think no, because I think
            -- there might be heartbeats too? so pull out type and match on that.
            let t = lookup "type" j
            case t of
              Just (JString "registration") => do
                old_state@(MkMatchState managers tasks) <- get MatchState
                put MatchState (MkMatchState (j :: managers) tasks)
                log "Put new manager into match state" 
                matchmake
              _ => ?error_unknown_registration_like_type
        zmq_poll_tasks_interchange_to_worker_loop tasks_interchange_to_worker_socket
      _ => ?error_tasks_to_interchange_expected_two_parts



-- TODO: is there anything to distinguish these three sockets at the type
-- level that ties into their expected use?
covering poll_loop : (State MatchState MatchState es, HasErr AppHasIO es) => ZMQSocket -> ZMQSocket -> ZMQSocket -> App es ()

covering app_main : (HasErr AppHasIO es, State MatchState MatchState es) => App es ()
app_main = do
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

  log "Creating worker task socket"
  tasks_interchange_to_worker_socket <- new_zmq_socket zmq_ctx ZMQSocketROUTER
  -- TODO: use configuration interchange address from submit process, rather
  -- than hard-coded 0.0.0.0, likewise for port
  zmq_bind tasks_interchange_to_worker_socket "tcp://0.0.0.0:9003"

  log "Created worker task socket"

  poll_loop command_socket tasks_submit_to_interchange_socket tasks_interchange_to_worker_socket

  -- TODO move these sockets into elixir style single state object

poll_loop command_socket tasks_submit_to_interchange_socket tasks_interchange_to_worker_socket = do
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
  -- TODO: these could have a "HasFD" or "IsPollable" interface rather than
  -- explicitly asking for the FD? So that you might specify: (Pollable, continuation) as a pair
  -- for the interface of this loop?
  tasks_submit_to_interchange_fd <- zmq_get_socket_fd tasks_submit_to_interchange_socket
  command_fd <- zmq_get_socket_fd command_socket
  tasks_interchange_to_worker_fd <- zmq_get_socket_fd tasks_interchange_to_worker_socket

  poll_outputs <- poll [MkPollInput command_fd 0,
                        MkPollInput tasks_submit_to_interchange_fd 0,
                        MkPollInput tasks_interchange_to_worker_fd 0]
                       (MkTimeMS (-1)) -- -1 means infinity. should either be infinity or managed as part of a timed events queue that is not fd driven?

  log "poll completed"

  -- one thing i dislike about the rust polling interface I used is it
  -- was ugly looking to track the fd (poll output entry) back to the
  -- originating socket - because I'm controlling the idris2 level poll
  -- API perhaps I can add in something more here? (eg I can copy
  -- arbitrary fields from the PollInput to PollOutput structure without
  -- unix poll() needing to know about them)

  -- I'm not really sure what that interface would look like. Potentially
  -- a much higher level "here's a callback to run for each one" rather
  -- than returning a structure?

  -- perhaps something like a list of fd, callback tuples (or 2nd value can be
  -- a free type) so that when poll comes back, we get a "thing" which is actually
  -- the content of the if-statements that would otherwise go below here.
  
  -- Semantics of buffer ownership: the caller must allocate a buffer and
  -- describe the size to zmq_recv. Probably some linear type stuff to be
  -- done here to ensure the buffer gets used properly? (maybe even type
  -- state for: not populated, populated by a message which can be read
  -- many times -> delete/reuse? or perhaps something region based to let
  -- the buffer be usable inside the region but not outside it?)
  -- TODO: biggest danger is making sure it is freed safely/appropriately...
  -- (after all uses, and eventually - pretty much 1-multiplicity semantics?)
  -- imagine an API: recvAndThen which takes a linear continuation?

  when ((index 0 poll_outputs).revents /= 0) $ do
    log "Got a poll result for command channel. Doing ZMQ-specific event handling."
    zmq_poll_command_channel_loop command_socket

  when ((index 1 poll_outputs).revents /= 0) $ do
    log "Got a poll result for tasks submit to interchange channel. Doing ZMQ-specific event handling."
    zmq_poll_tasks_submit_to_interchange_loop tasks_submit_to_interchange_socket

  when ((index 2 poll_outputs).revents /= 0) $ do
    log "Got a poll result for tasks interchange to workers channel. Doing ZMQ-specific event handling."
    zmq_poll_tasks_interchange_to_worker_loop tasks_interchange_to_worker_socket

  poll_loop command_socket tasks_submit_to_interchange_socket tasks_interchange_to_worker_socket

  log "Idris2 interchange ending"


covering main : IO ()
main = run (new (MkMatchState [] []) app_main)
