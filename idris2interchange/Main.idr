module Main

-- here's a kinda related effort (with webgl) that has a bunch
-- of journal-ish documentation:
-- https://drbearhands.com/idris-webgl/

-- wow it took a long time before importing a library module...
-- the source code was 290 lines long before adding the first
-- import...
import Control.App
import Data.Monoid.Exponentiation
import Data.Vect
import Generics.Derive
import System.FFI

import Bytes
import FD
import Logging
import Pickle
import ZMQ

%ambiguity_depth 10
%language ElabReflection
%default total

-- TODO: this should eventually be chosen according to
-- config supplied on stdin, or by random bind.
WORKER_TASK_PORT : Int
WORKER_TASK_PORT = 9003

-- for benc dev environment:
-- apt install chezscheme      -- to run idris
-- apt install libzmq3-dev     -- for my own zmq bindings
-- (build idris2 if necessary)
-- export PATH=~/.idris2/bin:$PATH
-- pytest -s parsl/tests/ --config parsl/tests/configs/htex_idris2.py 

data ManagerID = MkManagerId GCByteBlock
-- TODO: %runElab derive "ManagerID" [Generic, Meta, Show]
Show ManagerID where
  show m = "<manager-id show notimpl>"
-- this runElab doesn't work, i think because GCByteBlock is not
-- Show-able. But maybe ManagerID should have something more
-- explicitly ASCII-like.

record ManagerRegistration where
  constructor MkManagerRegistration
  manager_id : ManagerID
  manager_pickle : PickleAST  -- TODO: this manager_pickle can go away and be replaced by the two fields that are actually used.
%default covering
%runElab derive "ManagerRegistration" [Generic, Meta, Show]
%default total

data TaskDef = MkTaskDef PickleAST

%default covering
%runElab derive "TaskDef" [Generic, Meta, Show]
%default total

-- ^ default to covering because i can't see how to specify it as non-total, and otherwise:
-- parsl/tests/test_callables.py Error: implShowTaskDef is not total, possibly not terminating due to call to Pickle.implShowPickleAST


record MatchState where
  constructor MkMatchState
  managers: List ManagerRegistration
  tasks: List TaskDef    
-- %runElab derive "MatchState" [Generic, Meta, Show]

record SocketState where
  constructor MkSocketState
  command : ZMQSocket
  tasks_submit_to_interchange : ZMQSocket
  worker_to_interchange : ZMQSocket
  results_interchange_to_submit : ZMQSocket
  submit_pidfd : FD PidFD

-- this should be total, proved by decreasing n
-- but apparently not? cheat by using assert_smaller...
covering inner_ascii_dump : HasErr AppHasIO es => (1 _ : ByteBlock) -> App1 es ByteBlock
inner_ascii_dump bytes = do
 let (l # bytes) = length1 bytes
 case l of
   Z => pure1 bytes
   S _ => do
     (b # rest) <- bb_uncons bytes  -- we know this won't fail because we're in S _ case, but that isn't indicated at the type level...
     if b >= 32 && b < 128
      then app $ primIO $ putStr (singleton (chr $ cast b))
      else app $ primIO $ putStr "."
     inner_ascii_dump rest

covering ascii_dump : (State LogConfig LogConfig es, HasErr AppHasIO es) => (1 _ : ByteBlock) -> App1 es ByteBlock
ascii_dump v = do
  en <- app $ loggingEnabled
  app $ primIO $ pure () -- any use of IO here makes the bb_duplicate below work. otherwise, cannot resolve es... is this a BUG? what is the magic unification thingy doing?
  if en 
    then do  -- is this using the wrong bind? bb_duplicate works outside the if, and if i put it there, it makes the below bb_duplicate work too - as if a different elaboration of `do` / >>= is happening?
          (v # v') <- bb_duplicate v
          v' <- inner_ascii_dump v'
          free1 v'  -- is this v' actually freeable? it's not the original v' I think, even though it looks like I am hoping it is. because inner_ascii_dump advances the point through the byte block using bb_uncons. TODO greater thinking about how I want to manage byte blocks, in the context of how I actually use them. (for example, mutate vs share; reference count; ensure being freed)
          app $ primIO $ putStrLn ""
          pure1 v
    else pure1 v


-- TODO: remove the error paths. One way is to make this Maybe,
-- but then there is still a failing path to deal with where
-- get_block_id is called. More type-interesting would be to
-- replace the PickleAST with a manager data structure containing
-- the fields that are interesting, and this function becomes
-- the probably much simpler projection of that field out of
-- the manager record.
covering
get_block_id : ManagerRegistration -> PickleAST
get_block_id mr = 
  case manager_pickle mr of
    (PickleDict reg_dict) => 
      case lookup "block_id" reg_dict of
        Just s => s
        Nothing => ?error_block_id_missing
    _ => ?error_reg_dict_is_not_a_dict


-- the Python type contained in the returned AST depends on the supplied
-- command... TODO: maybe I can describe that typing in the idris2 code
-- rather than returning an equivalent to Python Any... so that the
-- individual dispatch_cmd pieces can be typechecked a bit?
covering dispatch_cmd : (State MatchState MatchState es, State LogConfig LogConfig es, HasErr AppHasIO es) => String -> App es PickleAST

dispatch_cmd "WORKER_BINDS" = do
  log "WORKER_BINDS requested"
  -- hard-code return value because port is also hard-coded ...
  -- TODO: this should be some environment to be passed around
  -- perhaps as a monad-style reader environment?
  pure $ PickleInteger $ cast WORKER_TASK_PORT

dispatch_cmd "CONNECTED_BLOCKS" = do
  log "CONNECTED_BLOCKS requested"
  -- In the rusterchange, it seemed like connected blocks didn't
  -- need implementing for the test suite, and could return an empty
  -- list.
  -- But! this is needed for surviving scaling timeout - without listing
  -- registered blocks here, a block is eventually marked as MISSING
  -- and fails.
  -- so registration now needs to store some state, and then return it here.
  -- (also in the same situations in the rusterchange and elixir interchange)

  -- TODO: This command is not properly tested - in the sense that a fast enough test run
  -- will not fail if CONNECTED_BLOCKS is not implemented. But a slow enough
  -- run will.

  -- Pull out the block_id of each of the managers_json list entries,
  -- and turn it into a unicode string.
  -- It's fine if this fails, because the implicit protocol says it will
  -- always be there. But it's ugly type-wise.

  -- There is no need to de-dupe, although probably should, and it would
  -- probably be an improvement (I think) on the real interchange - assuming
  -- de-dupe cost is less than the cost of sending them all and looking
  -- through them all on the client side. There are implementation methods
  -- that could be more efficient here too: rather than de-duping on each
  -- CONNECTED_BLOCKS, keep a set at registration time.
  -- See issue #3366 about that happening in the real interchange.

  (MkMatchState managers tasks) <- get MatchState
  let manager_block_ids = map get_block_id managers
  pure (PickleList manager_block_ids)

dispatch_cmd "MANAGERS" = do
  log "MANAGERS requested"
  -- TODO: notimpl, but maybe this is enough for tests?
  pure (PickleDict [])

dispatch_cmd c = do
  logv "Request with unknown command" c
  pure PickleNone


covering matchmake :  (State LogConfig LogConfig es, State MatchState MatchState es, HasErr AppHasIO es) => SocketState -> App es ()
matchmake sockets = do
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
    [] => log "No match possible: No tasks in matchmaker"
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
      -- TODO: there is no testing that the correct number of tasks are
      -- dispatched to each manager, in Parsl test suite.
      case managers of
        [] => log "No match possible: no managers registered"
        mr :: rest_managers => do
          logv "Considering manager for match" mr
          (PickleDict mpd) <- pure (manager_pickle mr)
            | _ => ?error_manager_pickle_is_not_a_dict -- TODO: tidy up by better manager_pickle type
          let c = lookup "max_capacity" mpd
          case c of
            Just (PickleInteger c) => do
              logv "This manager has capacity" c
              if c > 0
                then do
                  -- TODO: update the record to have one less available capacity unit
                  --       and if implementing ManagerLost, track task/manager allocation.

                  -- this task_msg looks like: 
                  --   self.task_outgoing.send_multipart([manager_id, b'', pickle.dumps(tasks)])

                  -- TODO: if we matchmake a lot of tasks at once, this singleton
                  -- list could contain many matched tasks destined for the same
                  -- manager.
                  let (MkTaskDef task_pkl) = task
                  let task_msg = PickleList [task_pkl]
                  logv "Dispatching task" task_msg
                  app1 $ do
                    resp_bytes <- pickle task_msg
                    -- TODO: something to do with read-only access here to b?
                    -- Its a byte buffer I want to share all over, and it is small, so maybe
                    -- it is interesting to have the garbage collector manage that, rather
                    -- than linear types?
                    -- these three sends demonstrate three different kind of memory policy
                    let (MkManagerId mid) = manager_id mr
                    b <- from_gc_bytes mid
                    b <- zmq_send_bytes1 sockets.worker_to_interchange b True
                    free1 b

                    -- this is the unused empty byte block mentioned in https://github.com/Parsl/parsl/issues/3372
                    app $ zmq_send_bytes sockets.worker_to_interchange emptyByteBlock True

                    resp_bytes <- zmq_send_bytes1 sockets.worker_to_interchange resp_bytes False

                    -- Here is an example of using linear types to ensure b and resp_bytes get freed.
                    -- This code won't compile without the free1 calls.
                    -- Compare that to how rust lifetimes would insert a free here.
                    -- also compare the resp_bytes <- action resp_bytes    syntax above to rust's
                    -- borrow syntax: we end up with the "same" variable bound. but it's not necessarily
                    -- the same because the action can return something else... (but not the same as a
                    -- mutable borrow)
                    free1 resp_bytes

                  put MatchState (MkMatchState managers rest_tasks)
                  matchmake sockets  -- this must be a tail call (can I assert that? I think not... but it would be nice)
                else ?notimpl_manager_oversubscribed
                  -- TODO: this never happens... because there is an unimplemented TODO to reduce the capacity...
                  -- when that happens, then this path should be reached.

                  -- Without manager decrement and this case, the tests still pass. there isn't a test failure when
                  -- all the tasks get sent to one manager. instead the behaviour is basically the same as if there
                  -- is a large prefetch value: they queue up and performance is changed (probably reduced, except
                  -- to the extent that prefetch increases it). Maybe that's interesting to note as untested?
            _ => ?error_registration_bad_max_capacity
                  -- TODO: a more strongly typed manager record (rather than arbitrary dict) would not have this case:
                  -- eg parse (don't validate) at registration time?


||| this drains ZMQ when there's a poll. this is a bit complicated and
||| not like a regular level-triggered poll.
||| Some reading:
||| https://funcptr.net/2012/09/10/zeromq---edge-triggered-notification/
||| https://github.com/zeromq/libzmq/issues/3641
covering zmq_poll_command_channel_loop : (State MatchState MatchState es, State LogConfig LogConfig es, HasErr AppHasIO es) => ZMQSocket -> App es ()
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
        unpickled_msg <- app1 $ do
          bytes <- zmq_msg_as_bytes msg
          v <- unpickle bytes
          pure v
        (PickleUnicodeString cmd) <- pure unpickled_msg
            | _ => ?error_cmd_is_not_a_string
        logv "Command received this command" cmd
        resp <- dispatch_cmd cmd
        logv "Response to command" resp
        app1 $ do
          resp_bytes <- pickle resp
          resp_bytes <- zmq_send_bytes1 command_socket resp_bytes False
          free1 resp_bytes

        -- after this send, command socket is back is ready-for-a-REQ state

        zmq_msg_close msg
        zmq_poll_command_channel_loop command_socket

-- TODO: factor ZMQ_EVENTS handling with other channels
covering zmq_poll_tasks_submit_to_interchange_loop : (State LogConfig LogConfig es, State MatchState MatchState es, HasErr AppHasIO es) => SocketState -> App es ()
zmq_poll_tasks_submit_to_interchange_loop sockets = do
  -- need to run this in a loop until it returns no events left
  events <- zmq_get_socket_events sockets.tasks_submit_to_interchange
  logv "ZMQ poll tasks submit to interchange loop got these zmq events" events

  -- TODO: ignoring writeability of this channel. I think thats the right
  -- thing to be doing: being ready for a write doesn't mean we can write
  -- to it immediately. (?)

  when (events `mod` 2 == 1) $ do  -- read
    log "Trying to receive a message from task submit->interchange channel"
    maybe_msg <- zmq_recv_msg_alloc sockets.tasks_submit_to_interchange
    case maybe_msg of
      Nothing => log "No message received on task submit->interchange channel"
      Just msg => do
        s <- zmq_msg_size msg
        logv "Received task-like message on task submit->interchange channel, size" s

        task_pkl <- app1 $ do
          bytes <- zmq_msg_as_bytes msg
          bytes <- ascii_dump bytes
          unpickle bytes

        let task = MkTaskDef task_pkl

        zmq_msg_close msg

        logv "Unpickled task" task

        -- TODO: add to match state

        old_state@(MkMatchState managers tasks) <- get MatchState
        -- logv "Old match state" old_state
        put MatchState (MkMatchState managers (task :: tasks))
        log "Put new task into match state" 
        matchmake sockets

        -- TODO: deallocate the message and bytes (after unpickling)

        -- TODO: so now we've received a message... we'll need to eventually deallocate
        -- and also do something with the message

        zmq_poll_tasks_submit_to_interchange_loop sockets


covering process_result_part : (State LogConfig LogConfig es, State MatchState MatchState es, HasErr AppHasIO es) => SocketState -> () -> ZMQMsg -> App es ()
process_result_part sockets () msg_part = do
 -- PARSL #3950: There are two tag dispatchers: one here and
 -- one in the caller to process_result_part. I think there
 -- only needs to be one tag in the protocol, and then one
 -- dispatcher in the interchange.

 -- Only forward on this if it is a result-tag
 -- (rather than eg. monitoring or heartbeat or registration)
 app1 $ do
  -- ISSUE: I couldn't get 'get MatchState' to work/typecheck here
  -- with the typechecker trying various other 'get' implementations
  -- (eg from list/vect) rather than the state one.
  -- It turns out its because I was omitting the `app` on the front of
  -- `get MatchState` so it was in the wrong monad-like (App vs App1)
  -- and the type error I got did not make that clear.
  bytes <- zmq_msg_as_bytes msg_part
  (bytes # bytes') <- bb_duplicate bytes
  result <- unpickle bytes
  logv "Unpickled result" result
  case result of
   PickleDict pairs => do
     let tag = lookup (PickleUnicodeString "type") pairs
     logv "result tag" tag
     case tag of
       Just (PickleUnicodeString "result") => do
         log "Result, so sending onwards"
         bytes' <- zmq_send_bytes1 sockets.results_interchange_to_submit bytes' False
         free1 bytes'
       _ => do
         logv "Ignoring unknown rtype tag" tag
         free1 bytes'
     
   _ => ?error_bad_pickle_object_on_worker_channel

  -- TODO: release worker/task binding/count

 zmq_msg_close msg_part
 pure ()

covering zmq_poll_worker_to_interchange_loop : (State LogConfig LogConfig es, State MatchState MatchState es, HasErr AppHasIO es) => SocketState -> App es ()
zmq_poll_worker_to_interchange_loop sockets = do
  -- TODO: do monitoring forwarding. test monitoring forwarding.
  -- TODO: do heartbeat processing. test heartbeat processing.
  events <- zmq_get_socket_events sockets.worker_to_interchange
  logv "ZMQ poll results worker to interchange loop got these zmq events" events
  
  when (events `mod` 2 == 1) $ do -- read
    log "Trying to receive a message from worker<->interchange channel"
    -- First part will be the manager ID. Then a pickle metadata dict.
    -- Subsequent parts will be individual results, for result type messages.
    msgs <- zmq_recv_msgs_multipart_alloc sockets.worker_to_interchange
    case msgs of
      [] => log "No message received on worker<->interchange channel"
      (addr_part :: meta_part :: other_parts) => do
        s <- zmq_msg_size addr_part
        logv "Received message on worker<->interchange channel, message size" s
        logv "Number of other parts in this multipart message" (length other_parts)

        addr_id <- app1 $ do
          remote_id <- zmq_msg_as_bytes addr_part
          remote_id <- ascii_dump remote_id
          to_gc_bytes remote_id
        zmq_msg_close addr_part

        meta_pkl <- app1 $ do
          bytes <- zmq_msg_as_bytes meta_part -- TODO: is this safe wrt message maybe being already closed, typechecking wise? i.e. does it copy the bytes or point to the bytes?
          unpickle bytes
        zmq_msg_close meta_part  -- TODO: something linear with zmq messages to enforce closing

        logv "Unpickled meta_pkl" meta_pkl

        -- ISSUE: without this `the`, can't figure out the type of the
        -- body of the case: it seems to be unable to resolve ?e with
        -- (). Could be something to do with having lots of holes for
        -- error handling introducing that ?e hole.
        the (App es ()) $ case meta_pkl of
         PickleDict pairs => do
           let tag = lookup (PickleUnicodeString "type") pairs
           logv "message meta type" tag
           case tag of

             Just (PickleUnicodeString "result") => do
               log "Result metatype"
               foldlM (process_result_part sockets) () other_parts 

             Just (PickleUnicodeString "registration") => do
               log "Registration metatype"
               (MkMatchState managers tasks) <- get MatchState
               let new_manager_registration = MkManagerRegistration (MkManagerId addr_id) meta_pkl
               put MatchState (MkMatchState (new_manager_registration :: managers) tasks)
               matchmake sockets

             Just (PickleUnicodeString "heartbeat") => do
               log "Heartbeat metatype -- ignoring, DANGER"
             _ => ?notimpl_meta_pkl_unknown_metatype_tag

         _ => ?error_protocol_meta_pkl_not_a_dict

        zmq_poll_worker_to_interchange_loop sockets
      _ => ?error_protocol_incorrect_msg_parts_case


-- TODO: is there anything to distinguish these three sockets at the type
-- level that ties into their expected use?
covering poll_loop : (State LogConfig LogConfig es, State Bool Bool es, State MatchState MatchState es, HasErr AppHasIO es) => SocketState -> App es ()


-- TODO: this only reads up to 128kb. which should be enough for test interchange configs. but is lame.
covering readStdinToEnd : HasErr AppHasIO es => App1 es ByteBlock
readStdinToEnd = read stdin 128000


covering readStdinConfig : (State LogConfig LogConfig es, HasErr AppHasIO es) => App es PickleAST
readStdinConfig = do
  log "Reading config"
  -- this is a Python pickle. in the Python side, it reads
  -- from the stream parsing until the natural end of the
  -- pickle at a STOP opcode (I think). but my pickle impl
  -- here wants a byte sequence - so I guess i will read until
  -- eof, and hope there really is an EOF (rather than the
  -- submit side leaving the stdin open)

  config <- app1 $ do
    bytes <- readStdinToEnd
    unpickle bytes

  logv "Config" config

  pure config

covering main_with_zmq : (State LogConfig LogConfig es, State Bool Bool es, HasErr AppHasIO es, State MatchState MatchState es) => PickleAST -> ZMQContext -> App es ()

covering app_main : (State LogConfig LogConfig es, State Bool Bool es, HasErr AppHasIO es, State MatchState MatchState es) => App es ()
app_main = do
  log "Idris2 interchange starting"

  -- TODO: turn this into a reader app type rather than threading it around?
  cfg <- readStdinConfig

  -- could use some with-style notation? zmq context doesn't need any cleanup
  -- if the process will just be terminated, and I think it doesn't have much
  -- in the way of linearity constraints? (although probably the sockets do?)
  -- but to make clean shutdown clean (eg maybe to make valgrind happier).
  -- I do care about the context not being usable outside of a with-style block
  -- which is something I've seen done with region typing in Haskell.
  with_zmq_context $ \zmq_ctx => do
    main_with_zmq cfg zmq_ctx

  log "Idris2 interchange ending gracefully"

main_with_zmq cfg zmq_ctx = do
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

  log "Creating results interchange->submit socket"
  results_interchange_to_submit_socket <- new_zmq_socket zmq_ctx ZMQSocketDEALER
  zmq_connect results_interchange_to_submit_socket "tcp://127.0.0.1:9001"

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
  -- TODO: these URLs should be typed, not strings, and should be
  --       configured from the supplied stdin config
  zmq_connect command_socket "tcp://127.0.0.1:9002"
  log "Connected interchange command channel"

  log "Creating worker task socket"
  worker_to_interchange_socket <- new_zmq_socket zmq_ctx ZMQSocketROUTER
  -- TODO: use configuration interchange address from submit process, rather
  -- than hard-coded 0.0.0.0, likewise for port
  zmq_bind worker_to_interchange_socket "tcp://0.0.0.0:9003"

  log "Created worker task socket"

  -- This `pure` is to get access to bind/| syntax which I think doesn't exist
  -- for `let` -- but I haven't checked for sure?
  PickleDict config_dict <- pure cfg
    | _ => ?error_config_is_not_a_dict

  -- TODO: overload strings so that can say lookup "submit_pid" without
  -- the PickleUnicodeString constructor
  (Just (PickleInteger submit_pid)) <- pure $ lookup (PickleUnicodeString "submit_pid") config_dict
    | _ => ?error_pidfd_is_not_an_int

  logv "Submit pid" submit_pid

  log "Creating submitter pidfd"
  -- There's a race condition in this approach (as with any "name the process
  -- with a pid" approach) that the process might be gone and replaced by a
  -- new unrelated process by this time.

  -- TODO: cast here hopes that the integer is small enough to fit into the
  -- pid space...
  submit_pidfd <- pidfd_open (cast submit_pid)

  logv "Created submitter pidfd" submit_pidfd

  -- TODO: is there a named record syntax for construction?
  -- I can't find one? I want this for the same reason
  -- as I want Python kwonly args - maintainable style
  let sockets = MkSocketState {
       command = command_socket,
       tasks_submit_to_interchange = tasks_submit_to_interchange_socket,
       worker_to_interchange = worker_to_interchange_socket,
       results_interchange_to_submit = results_interchange_to_submit_socket,
       submit_pidfd = submit_pidfd
       }

  poll_loop sockets

  -- TODO: this is perhaps tight enough usage that a
  -- `with_socket` style could be implemented? With linear
  -- type? because that's a bit more sessiony? and would
  -- let me put a protocol state on the command socket too.
  -- (for in waiting for req, waiting for rep states)
  close_zmq_socket command_socket
  close_zmq_socket tasks_submit_to_interchange_socket
  close_zmq_socket worker_to_interchange_socket
  close_zmq_socket results_interchange_to_submit_socket


poll_loop sockets = do


  -- fairly ugly hack: before asking the OS to wait for poll, the
  -- individual ZMQ sockets get their events checked: an fd event
  -- does not always happen because other zmq socket operations
  -- somehow cancel that (as documented in the ZMQ_FD socket option)
  -- so we need to do a zmq-level event check for them if that happens.
  -- It is harmless (if maybe inefficient) to do this for all the
  -- zmq sockets here - although some other flag system might be
  -- usable too, that is set whenever we interact with a socket?
  -- This is drifting towards having multiple kinds of event in one
  -- loop which i was hoping to avoid with using polling fds. But as
  -- long as this loop doesn't have to keep going round when nothing
  -- is happening, i guess its OK to do one-off checks for lots of
  -- stuff before the poll.
  -- This code could maybe be structured as a callback that happens
  -- both when the poll indicates it is time too look, and also
  -- immediately before the poll happens? so then there is only a
  -- single callback declaration?

  zmq_poll_command_channel_loop sockets.command
  zmq_poll_tasks_submit_to_interchange_loop sockets
  zmq_poll_worker_to_interchange_loop sockets



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

  -- so first we need to ask those three sockets for their FDs
  -- TODO: these could have a "HasFD" or "IsPollable" interface rather than
  -- explicitly asking for the FD? So that you might specify: (Pollable, continuation) as a pair
  -- for the interface of this loop?
  tasks_submit_to_interchange_fd <- zmq_get_socket_fd sockets.tasks_submit_to_interchange
  command_fd <- zmq_get_socket_fd sockets.command
  worker_to_interchange_fd <- zmq_get_socket_fd sockets.worker_to_interchange

  poll_outputs <- poll [MkPollInput command_fd 0,
                        MkPollInput tasks_submit_to_interchange_fd 0,
                        MkPollInput worker_to_interchange_fd 0,
                        MkPollInput (submit_pidfd sockets) 0]
                       (MkTimeMS (-1)) -- -1 means infinity. should either be infinity or managed as part of a timed events queue that is not fd driven?

  log "poll completed"


  log "DEBUG: reading tasks interchange to worker status"
  logv "DEBUG: socket poll result: " (index 2 poll_outputs).revents
  dbg_events <- zmq_get_socket_events sockets.worker_to_interchange
  logv "DEBUG: zmq socket events: " dbg_events

  -- TODO: some assert here?

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
    zmq_poll_command_channel_loop sockets.command

  when ((index 1 poll_outputs).revents /= 0) $ do
    log "Got a poll result for tasks submit to interchange channel. Doing ZMQ-specific event handling."
    zmq_poll_tasks_submit_to_interchange_loop sockets

  when ((index 2 poll_outputs).revents /= 0) $ do
    log "Got a poll result for tasks interchange to workers channel. Doing ZMQ-specific event handling."
    zmq_poll_worker_to_interchange_loop sockets

  -- TODO: it's horrible API-wise that these numbers are manually tied to
  -- position in the poll list -- removing the worker to interchange result
  -- socket needed manual renuber of the subsequent pidfd handler!
  -- Implement an API that looks more like a match/case style poll call?

  when ((index 3 poll_outputs).revents /= 0) $ do
    log "Got a pidfd event from submit process. Setting exit state."
    put Bool False

  continue <- get Bool
  if continue
    then poll_loop sockets
    else log "Ending poll loop"


covering main : IO ()
main = run (new True
           (new (MkLogConfig True)
           (new (MkMatchState [] []) app_main)))
