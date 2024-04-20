use byteorder::ReadBytesExt;
use queues::IsQueue;
use std::io::BufRead;

// Vocab:

// This document attempts to avoid the use of "client" and "server" as
// there are multiple interpretations for that even on a single connection,
// and htex is not a clear client server architecture.
// TODO: in Python interchange, "client" is used a lot and can be a bit confusing - it usually means the main process of parsl that is submitting tasks into the interchange, and in this rust impl, that's called "submit" not "client.
// CURVE security also has a notion of server and client, because communications are bootstrapped needing to have the servers public key well known (and correct), but not the clients. I think it's not necessarily that case that this client/server direction aligns with either the TCP connection direction or REQ/REP direction.

// TODO: terminology needs clarifying and consistenifying: managers, pools, workers. are we sending things to/from a "pool" or "manager"? is the ID for a "manager" or for a "pool"?

// threadedness: single threaded as much as possible. I initially thought I'd use async rust for this, but a poll-loop has been how things have naturally flushed out for me. That perhaps also reflects on how I thought the Python interchange should perhaps use async Python instead of threads, but actually might be better fully poll driven. CURVE in the Python interchange uses an auth thread, but the communications are all done over zmq sockets so does that mean I can implement that socket in my regular poll loop?  libzmq might be using threads internally though? but we hopefully aren't introducing thread related code in the rust interchange implementation itself.   using async might make it easier to understand some of the different bits of code if they start getting too tangled together, but I think everything is message or time driven - there aren't any state machines to manually construct that async would help with, I think? that doesn't mean I can't use it gratuitously, though.

// TODO: there's a multiprocessing Queue used at start-up that this interchange does not implement
// I should replace it with a PORTS command I think? In the prototype it's hacked out and only works with hard-coded ports.
// Removing multiprocessing fork and perhaps using a regular python fork/exec would force this to happen anyway? issue #2343
// That doesn't work with proposal to flip direction of command channel...
// Some other stuff (maybe wq/taskvine?) outputs a port number on stdout/stderr at startup after binding.

// TODO: theres an implicit "we have exited" channel (perhaps with a unix exit code?) that the submit side could pay attention to, to notice if the interchange is gone away, either accidentally or deliberately.

// TODO: this code has no handling/reasoning about what happens when any ZMQ queue is unable to deal with a `send` call (aka its full)
//       and the Python interchange doesn't have clear documentation about what's meant to be happening then either.
//       For example, should we hold off on task matching now waiting instead for a POLLOUT on the channel we want?
//       That means instead of queueing things in a full ZMQ queue, we're just otherwise queuieng things inside some other queues, and queues to populate queues is suspicious... alternatives... if it's a task submission, fail that task in the same way as we fail other tasks.
// TOOD: this code has no handling/reasoning about what happens if a ZMQ socket doesn't actually connect: as I've seen both in real Python Parsl and while developing this implementation, there's a lot of silent hangs hoping things get better - ZMQ Monitoring Sockets give some interersting progress information about socket connections which TODO could be logged in the Python interchange too.
// TODO: heartbeats are not implemented. there are a few heartbeats that are distinct, and perhaps it would be good to name them, "this is the heartbeat that detects XXX failing". heartbeats also need to pickle new python objects (the exception for missing task) which might be simple enough to do here as there isn't much in there? The basic htex_local.py test passes ok without any heartbeat implementation, though! TODO: check heartbeats really get tested.

// TODO: Parsl monitoring (not ZMQ monitoring) - both node table messages from the interchange and relaying on htex-radio messages from workers, over ZMQ to configured radio: that's something a bit more interesting Parsl-wise because I want to transition that to something configurable-in-Python-code which is not easy to replicate in Rust - for example, perhaps recognising the specific config objects for a few radios, and interpreting them, rather than running arbitrary Python code? And perhaps that should be a requirement for the radio specification?

// TODO: in development, this interchange often panics and exits, and that leaves htex in a hung state, rather than noticing and failing the tests: the user equivalent of that is eg. if oom-killer kills the real Python interchange, a run will hang rather than report an error... everything else has heartbeats but not this... (or even process-aliveness-checking...). htex submit side should notice a gone-away interchange by noticing the process is gone.

// TODO: this code has no handling of if one of a pair of sockets binds: for example (I think in github already) a worker can register and receive tasks (using its task port) but not send results because the result port may be misconfigured. This is an argument to move towards using a single channel per worker.

// TODO: if there's an encrypted yes/no misconfiguration between submit side and interchange, where submit side has encryption on and interchange does not, Parsl execution hangs. (maybe the other way round too - I didn't try). This should get i) fixed and ii) tested in mainline Parsl. ZMQ Monitoring gives some quite interesting progress messages which might help debugging.

// TODO: Auth: turns out if you don't bind an auth socket, zmq defaults to authorizing everyone (although in the interchange case,
// I think they would still need to acquire the servers public key - which is now a secret key, as far as that defence is concerned).

// TODO: note when sending python simple dicts, this is compatible across many python versions: for example, we can look at them in rust! and do.

// use of Queue wants Clone trait, which is a bit suspicious: does that mean we have
// explicit clones of the (potentially large) buffer in interchange? when ideally we
// would pass around the buffer linearly without ever duplicating it? TODO
// I'm doing a clone manually anyway, I guess I always need at least one
// memory copy to be able to get it out from the memory buffer of ZMQ...
#[derive(Clone)]
struct Task {
    task_id: i64,
    buffer: Vec<u8>,
}

#[derive(Clone)]
struct Slot {
    manager_id: Vec<u8>,
}

fn main() {
    println!("the rusterchange");

    // TODO: use a proper command line args library
    // This impl. requires a cert dir
    let mut args_iterator = std::env::args();
    let _ = args_iterator.next(); // skip the executable name
    let cert_dir = args_iterator.next().expect("certificate directory");
    println!("cert_dir is {}", cert_dir);

    // the cert_dir has these four files in it:
    // client.key  client.key_secret  server.key  server.key_secret

    // "client" and "server" terminology here refers to the CURVE client/server
    // directions (which may not align with either TCP or REQ/REP direction).
    // parsl.curvezmq implements these two directions as new ZMQ context facades
    // and then every socket created via one of those facades is aligned in
    // direction with the parsl.curvezmq facade context (ServerContext or
    // ClientContext). But that's a Parsl library-impl thing, not a ZMQ thing,
    // and not an interchange protocol thing.
    // In interchange protocols, the interchange end of the connection is always
    // a CURVE server.
    // CURVE server sockets need to know their own secret key, but they don't
    // need to know anything about the client.
    // However, the client public key (which will be used as the client identity)
    // needs to be known to the authentication code which will be invoked by
    // ZMQ on new connections.

    // so for now, load only the server secret key
    // this is encoded in ZPL and there's no parser for that. so let's build a
    // minimal one that's only enough to read the keys (rather than
    // read arbitrary ZPL files)

    let server_keypair = load_zmq_keypair(&std::path::Path::new(&cert_dir).join(std::path::Path::new(&"server.key_secret")));


    // we've got 8 communication channels:

    // there are 5 ZMQ channels:
    //   three to the submit side (sometimes called the client side) - TCP outbound
    //        TODO: could these be ZMQ IPC sockets? how would that compare to using TCP
    //              here? Would it give us, for example, the unix user security model
    //              which TCP doesn't have? Could some of the Parsl monitoring stuff be
    //              done that way too?
    //
    //   two to the worker side - TCP listening  (x number of worker pools)
    //     TODO: this should be unified into one? I think the performance reasons for
    //           having two sockets have gone away - primarily, i think, because
    //           a new task is not dispatched until a result is processed, rather than
    //           them being so decoupled.
    //
    // there's also a startup-time channel that is in master parsl a multiprocessing.Queue
    // that sends the port numbers for the two listening ports chosen by the interchange
    // at zmq socket bind time.
    //
    // there are various command line arguments which communicate configuration information
    // into the interchange to help it get bootstrapped with ZMQ channels.
    //
    // ... and we shutdown on unix termination signals, not on a message - this probably
    // should be thought of as a very limited communication channel, when reasoning about
    // whole system behaviour.

    let zmq_ctx = zmq::Context::new();
    // will be destroyed by implicit destructor, i think

    // This channel is a DEALER socket paired with a dealer socket on the submit
    // side.
    // This channel will convey pickle formatted messages {task_id: , buffer: }
    // without expecting a response back on that channel: responses to this message
    // will be the result of attempting to execute this task, which will be conveyed
    // on the zmq_results_interchange_to_submit channel.
    // The DEALER multiple-endpoint routing behaviour is not used and not supported: nothing
    // conveys results back to where they came from if multiple clients are connected
    // here, and the submit side code makes assumptions that it is only connected to
    // a single interchange - so this is only used in a 1:1 configuration.
    let zmq_tasks_submit_to_interchange = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create task_submit_to_interchange socket");
    println!("gcs {}", zmq_tasks_submit_to_interchange.is_curve_server().expect("no fail to read"));
    zmq_tasks_submit_to_interchange.set_curve_server(true).expect("setting as curve server");
    zmq_tasks_submit_to_interchange.set_curve_secretkey(&server_keypair.secret_key).expect("setting server secret key");
    zmq_tasks_submit_to_interchange
        .connect("tcp://127.0.0.1:9000")
        .expect("could not connect task_submit_to_interchange socket");

    zmq_tasks_submit_to_interchange
        .monitor(
            "inproc://monitor-tasks-submit-to-interchange",
            zmq_sys::ZMQ_EVENT_ALL
                .try_into()
                .expect("zmq_sys vs zmq API awkwardness"),
        )
        .expect("Configuring zmq monitoring");


    let zmq_tasks_submit_to_interchange_monitor = zmq_ctx
        .socket(zmq::SocketType::PAIR)
        .expect("create pair socket for zmq monitoring");
    zmq_tasks_submit_to_interchange_monitor
        .connect("inproc://monitor-tasks-submit-to-interchange")
        .expect("connecting monitoring");

    // this channel is for sending results from the interchange to the submit side.
    // messages on this channel are multipart messages. each part is a pickled Python dictionary
    // with a type field, a Python string. In this Rust implementation, probably only one part will be sent in each message.
    // TODO: is there an advantage to making these multipart messages rather than sending individual messages and processing them one by one?
    // types:
    //   'result': this have at least a task_id field
    //     which corresponds to the task_id in a task submission message
    //    when an exception is being sent (as is created by the Python interchange in some situations, not just passing along an exception from a worker pool) then the 'exception' field is set in this dictionary, and that should be a Parsl-serialized (not pickle-serialized, but using the parsl serialization abstractions) exception object.
    let zmq_results_interchange_to_submit = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create results_interchange_to_submit socket");
    zmq_results_interchange_to_submit.set_curve_server(true).expect("setting as curve server");
    zmq_results_interchange_to_submit.set_curve_secretkey(&server_keypair.secret_key).expect("setting server secret key");
    zmq_results_interchange_to_submit
        .connect("tcp://127.0.0.1:9001")
        .expect("could not connect results_interchange_to_submit socket");

    // this is a ZMQ REP socket, paired with a REQ socket on the submitting side
    // providing a command server in the interchange to which the submit side
    // sends a range of commands.
    // Commands are sent as pickled Python objects, and replies are returned as
    // pickled Python objects.
    //
    // Although commands from from submit side to interchange side, the TCP direction
    // is the other way round: the interchange makes a TCP connection into the
    // submit side and listens for commands on that connection. TODO: the use of the
    // command channel on the submit side is not thread safe, even with locks. So
    // what might be done differently? One option is one zmq connection per thread that
    // wants to run commands, which should then more easily be initiated the other way
    // round from submit end to interchange end? (another is to make the command
    // queue only be used from a single thread, and have some inside-process behaviour
    // to coordinate those RPCs across threads...). That's not so easy when we don't
    // know the port which will be opened (if random port is chosen on the interchange
    // side, to receive connections)...
    //
    // There's no requirement that command REQ/REP has to happen all on one channel,
    // though: there can be an initial configuration main thread REQ/REP that the
    // interchagne connects to, and then other threads on the submit side could connect
    // the other way (as a somewhat weird connection pattern...)
    //
    // This rust code probably doesn't implement all the commands - just as I
    // find my progress stopped by a missing command, I'll implement the next one.
    // Some commands are (as python pickled values) -- see _command_server in interchange.py
    //    "CONNECTED_BLOCKS"  -- return a List[str] connecting block IDs for every block that has connected. Blocks might be repeated (perhaps once per manager?)   TODO: that's probably a smell in the protocol: with thousands of nodes, this would make a 1-block message contain thousands of strings.
    //    "MANAGERS" -- return List[Dict]: one entry per known manager, each dict is some status about that manager, in an ad-hoc format
    //    "OUTSTANDING_C" -- return count of outstanding tasks (in queues and on workers) -- TODO: this info is available on the submit side, with a slight phase shift (more because also includes executor->interchange send queue, does not include interchange->executor result queue) - issue #3365
    let zmq_command = zmq_ctx
        .socket(zmq::SocketType::REP)
        .expect("could not create command socket");
    zmq_command.set_curve_server(true).expect("setting as curve server");
    zmq_command.set_curve_secretkey(&server_keypair.secret_key).expect("setting server secret key");
    zmq_command
        .connect("tcp://127.0.0.1:9002")
        .expect("could not connect command socket");

    // the bind addresses for these two ports need to be the self.interchange_address, not localhost
    // in order to accept connections from remote workers

    // This is a bi-directional channel, despite the name.
    // In the interchange to worker direction: this carries tasks to be
    //    executed.
    //     This is a ROUTER socket and so the first part of a send_multipart
    //     should be be the manager ID to receive the message.
    //     When the message contains tasks, the second part of the
    //     send_multipart should be a 0-length byte sequence and the third
    //     part should be a pickled Python object, List[Dict], where the dicts
    //     have a buffer and task_id attribute (similar but different to as received on the
    //     tasks_submit_to_interchange channel. TODO note that this is a list of tasks, while tasks_submit_to_interchange carries at most one task per message. TODO that cardinality mismatch could be made more consistent in the protocols.
    //     This implementation, which does per-slot matchmaking, probably won't send more than a single task at once in the list, though.
    //     Other messages (heartbeat and drain) can be sent on this channel, using magic task IDs. TODO: make messages use type tags in this channel
    //     TODO: it's unclear why this protocol has a blank byte string? it's always discarded... probably remove it?

    // In the workers to interchange direction:
    //    json formatted messages, not pickle formatted messages:
    //    the format of those messages is a json dict with a 'type' key: registration, heartbeat, drain
    //    This is a ROUTER socket and so receives from this message should be a multipart receive,
    //    with the first part being the sending manager ID and the second part being the JSON message.
    // In some cases, a registration message here from a worker with a different parsl version will cause the Python interchange to exit. TODO: what happens to Parsl in this case? is that code path tested? should it really be the case that the interchange shuts down? rather than passes on some other error back, but continues to run? (the assumption, i think, is that if *one* registration is wrong, that probably implies that the user has configured *all* registrations wrong - which is a reasonable assumption in most cases)
    let zmq_tasks_interchange_to_workers = zmq_ctx
        .socket(zmq::SocketType::ROUTER)
        .expect("could not create tasks_interchange_to_workers socket");
    zmq_tasks_interchange_to_workers.set_curve_server(true).expect("setting as curve server");
    zmq_tasks_interchange_to_workers.set_curve_secretkey(&server_keypair.secret_key).expect("setting server secret key");
    zmq_tasks_interchange_to_workers
        .bind("tcp://127.0.0.1:9003")
        .expect("could not bind tasks_interchange_to_workers");

    // In the workers to interchange direction, this carries results from tasks which were previously sent over the tasks_workers_to_interchange channel. Messges take the form of arbitrary length multipart messages. The first part as received from recv_multipart will be the manager ID (added by ZMQ because this is a ROUTER socket) and then each subsequent part will be a pickle containing a result. Note that this is different from the wrapping used on tasks_interchange_to_workers, where a single pickle object is sent, containing a pickle/python level list of task definitions. TODO: consistentify the multipart vs python list form.
    // The pickled object is a Python dictionary with a type entry that is one of these strings:  'result' 'monitoring' or 'heartbeat'.
    // The rest of the dictionary depends on that type.
    // TODO: this is a pickled dict, vs tasks_interchange_to_workers
    // TODO: this channel could be merged with tasks_interchange_to_workers, issue #3022, #2165
    let zmq_results_workers_to_interchange = zmq_ctx
        .socket(zmq::SocketType::ROUTER)
        .expect("could not create results_workers_to_interchange socket");
    zmq_results_workers_to_interchange.set_curve_server(true).expect("setting as curve server");
    zmq_results_workers_to_interchange.set_curve_secretkey(&server_keypair.secret_key).expect("setting server secret key");
    zmq_results_workers_to_interchange
        .bind("tcp://127.0.0.1:9004")
        .expect("could not bind results_workers_to_interchange");

    let mut task_queue: queues::Queue<Task> = queues::Queue::new();
    let mut slot_queue: queues::Queue<Slot> = queues::Queue::new();

    let mut manager_info: std::collections::BTreeMap<Vec<u8>, ()> =
        std::collections::BTreeMap::new();

    loop {
        // TODO: unclear to me what it means to share this sockets list across multiple loop iterations?

        // choose what to poll for based on if we want to be able to send things (in which case
        // poll for POLLOUT) otherwise we don't care about POLLOUT... I'm a bit unclear how much
        // data can be written when we've got a POLLOUT?

        // alas because of move semantics, these are not re-usable...
        let zmq_tasks_submit_to_interchange_poll_item =
            zmq_tasks_submit_to_interchange.as_poll_item(zmq::PollEvents::POLLIN);
        let zmq_tasks_interchange_to_workers_poll_item =
            zmq_tasks_interchange_to_workers.as_poll_item(zmq::PollEvents::POLLIN); // see protocol description for why we should be POLLIN polling on what sounds like its a send-only channel
        let mut sockets = [
            zmq_tasks_submit_to_interchange_poll_item,
            zmq_tasks_interchange_to_workers_poll_item,
            zmq_command.as_poll_item(zmq::PollEvents::POLLIN),
            zmq_results_workers_to_interchange.as_poll_item(zmq::PollEvents::POLLIN),
            zmq_tasks_submit_to_interchange_monitor.as_poll_item(zmq::PollEvents::POLLIN),
        ];

        // TODO: these poll items are referenced by indexing into sockets[n] which feels
        // pretty fragile - it's because the poll items are moved into the sockets array
        // rather than the array keeping references (because that's what the API for poll
        // is). That feels quite statically-fragile for getting the poll number and the
        // actual socket activities out of sync? is there any nice way to move them back,
        // borrow-style?

        println!("Polling");
        let count = zmq::poll(&mut sockets, -1).expect("poll failed");
        // -1 means infinite timeout, but maybe we should timeout on the heartbeat periods
        // in order to send and receive heartbeats? TODO

        println!("Found {} interesting socket(s)", count);

        // tasks submit to interchange channel

        if sockets[0].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("Poll result: there is a task from the submit side");
            let task = zmq_tasks_submit_to_interchange
                .recv_bytes(0)
                .expect("reading task message");
            let t = serde_pickle::de::value_from_slice(&task, serde_pickle::de::DeOptions::new())
                .expect("unpickling");
            // the protocol on this channel gives a dict with two entries:
            // a "buffer" and a "task_id"
            // if this protocol was declared as a dataclass or typed dict, would I be
            // able to generate a rust-side image of that class and then deserialize
            // into something I can interact with? (what does in-process rust binding
            // do in this situation?)

            let serde_pickle::Value::Dict(task_dict) = t else {
                panic!("protocol violation")
            };
            let serde_pickle::Value::I64(task_id) =
                &task_dict[&serde_pickle::HashableValue::String("task_id".to_string())]
            else {
                panic!("protocol violation")
            };
            let serde_pickle::Value::Bytes(buffer) =
                &task_dict[&serde_pickle::HashableValue::String("buffer".to_string())]
            else {
                panic!("protocol violation")
            };

            println!("Received htex task {}", task_id);

            // perhaps consider that i'll want to do richer matchmaking based on resource stuff contained in protocol (see outreachy internship) and be prepared to use something richer than a queue?
            // if just matchmaking based on queues without looking at the task, don't need to do any deserialization here... the pickle can go into the queue and be dispatched later.

            let task = Task {
                task_id: *task_id, // TODO: why need this *? something to do with ownership I don't understand
                buffer: buffer.clone(), // TODO: awkward clone here of buffer but I guess because of serde_pickle, we have to clone it out of the task_dict value if we're doing shared values... perhaps there is a way to convert the task dict into the buffer forgetting everything else, linearly? TODO
            };
            task_queue.add(task).expect("queue broken - eg full?");
        }

        if sockets[1].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("reverse message on tasks_interchange_to_workers");
            // this is JSON, not pickle
            let message = zmq_tasks_interchange_to_workers
                .recv_multipart(0)
                .expect("reading worker message from tasks_submit_to_interchange channel");
            let manager_id = &message[0];
            let json_bytes = &message[1];
            let json: serde_json::Value =
                serde_json::from_slice(json_bytes).expect("protocol error");
            println!("Message from workers to interchange: {}", json); // TODO: log the manager ID too... awkward because it's a byte string
            let serde_json::Value::Object(msg_map) = json else {
                panic!("protocol error")
            };
            let serde_json::Value::String(ref msg_type) = msg_map["type"] else {
                panic!("protocol error")
            };
            println!("Message type is: {}", msg_type);
            if msg_type == "registration" {
                println!("processing registration");
                // I think all we need from this message is the worker capacity.
                // There's also a uid field which is a text representation of the manager id. In the Python interchange, this field is unused - the manager_id coming from zmq as a byte sequence is used instead. TODO: assert that they align here. perhaps remove from protocol in master Parsl?
                let serde_json::Value::Number(ref capacity_json) = msg_map["max_capacity"] else {
                    panic!("protocol error")
                }; // max_capacity = worker_count + prefetch_capacity
                   // might be interesting to assert or validate that here as a protocol behaviour? TODO

                // now we're in a position for match-making
                // let's do that as a queue of manager requests, so that we have capacity copies of a Slot, that will be matched with Task objects 1:1 over time: specifically *not* keeping manager capacity as an int, but more symmetrically structured as two queues being paired/matched until one is empty. As a trade-off, this probably makes summary info more awkward to provide, though.
                let capacity = capacity_json.as_u64().expect("protocol error");
                for _ in 0..capacity {
                    println!("adding a slot");
                    slot_queue
                        .add(Slot {
                            manager_id: manager_id.clone(),
                        })
                        .expect("enqueuing slot on registration");
                }
                // TODO: it's an error for a manager ID to be used... is that a protocol error? or some other error?
                manager_info.insert(manager_id.clone(), ());
            } else {
                panic!("unknown message type")
            };
        }

        if sockets[2].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("command received from submit side");
            // this a REQ/REP pair, with this end being a REP, so we MUST
            // send back a single response message.
            let cmd_pickle_bytes = zmq_command.recv_bytes(0).expect("reading command message");
            let cmd = serde_pickle::de::value_from_slice(
                &cmd_pickle_bytes,
                serde_pickle::de::DeOptions::new(),
            )
            .expect("unpickling");
            println!("Unpickled command: {}", cmd);
            let resp_pkl = if cmd == serde_pickle::Value::String("CONNECTED_BLOCKS".to_string()) {
                // TODO: this needs to return all blocks that have ever had a manager connect,
                // even for blocks that no longer have a manager connected.
                serde_pickle::ser::value_to_vec(
                    &serde_pickle::value::Value::List([].to_vec()),
                    serde_pickle::ser::SerOptions::new(),
                )
                .expect("pickling block list")
            } else if cmd == serde_pickle::Value::String("MANAGERS".to_string()) {
                // Right now I'll only implement MANAGERS information to the extent needed for making tests pass,
                // rather than some more abstract notion of "behaving correctly"
                // TODO: this means we're going to need to remember the managers in a structure (because so far,
                // only remembering church-style slot counts in a queue)
                // TODO: Later on, we'll need to know which tasks went to a manager (rather than only a task count on
                // each manager) so that heartbeat failure can send the appropriate failure messages for the tasks it
                // is failing out.

                // The per-manager dictionary is formatted like this:
                //      resp = {'manager': manager_id.decode('utf-8'),       what format is this? some kind of string repr of manager ID?
                //                                                           TODO:  if it's not a byte sequence, this is a protocol wart
                //              'block_id': m['block_id'],                   block IDs... string or int?
                //              'worker_count': m['worker_count'],           int
                //              'tasks': len(m['tasks']),                    int - this can be bigger than worker count because of
                //                                                                 prefetch capacity but should never be bigger than
                //                                                                 worker count + prefetch capactity. Note that
                //                                                                 prefetch capacity is not reported in this message.
                //              'idle_duration': idle_duration,              float - number of seconds idle, 0 if not idle
                //                                                              - i guess protocol assertion, if tasks > 0,
                //                                                                this value must be 0? so sum-type representation
                //                                                                might be Activity = Idle seconds | Active [tasks]
                //              'active': m['active'],                       bool: active, in the sense of should we send tasks?
                //              'draining': m['draining']}                   bool: draining, in the sese of should we send tasks?
                //                                                             - there's some subtle behavioural difference here which
                //                                                               could be sorted out, but future draining work might
                //                                                               remove the notion of a draining bool and instead have
                //                                                               a draining end-time that is always or often set. TODO
                // TODO: return actual manager info, not an empty list
                serde_pickle::ser::value_to_vec(
                    &serde_pickle::value::Value::List([].to_vec()),
                    serde_pickle::ser::SerOptions::new(),
                )
                .expect("pickling MANAGERS list")
            } else {
                panic!("This command is not implemented")
            };
            zmq_command
                .send(resp_pkl, 0)
                .expect("sending command response");
        }

        if sockets[3].get_revents().contains(zmq::PollEvents::POLLIN) {
            let parts = zmq_results_workers_to_interchange
                .recv_multipart(0)
                .expect("couldn't get messages from results_workers_to_interchange");
            println!("Received a result-like message with {} parts", parts.len());
            // this parts vec will contain first a manager ID, and then an arbitrary number of result-like parts from that manager.
            let mut parts_iter = parts.into_iter();
            let manager_id = parts_iter.next().expect("getting manager ID");
            for part_pickle_bytes in parts_iter {
                println!("Processing part");
                // part is a pickled python dict with a 'type' tag
                // a part here is not necessarily a result: this channel also contains heartbeats and monitoring messages.
                // TODO: implement monitoring handling
                // TODO: implement heartbeat handling
                let p = serde_pickle::de::value_from_slice(
                    &part_pickle_bytes,
                    serde_pickle::de::DeOptions::new(),
                )
                .expect("protocol error");

                let serde_pickle::Value::Dict(part_dict) = p else {
                    panic!("protocol violation")
                };
                let serde_pickle::Value::String(part_type) =
                    &part_dict[&serde_pickle::HashableValue::String("type".to_string())]
                else {
                    panic!("protocol violation")
                };

                println!("Result-like message part type: {}", part_type);
                if part_type == "result" {
                    // pass the message on without reserializing it
                    zmq_results_interchange_to_submit
                        .send_multipart([&part_pickle_bytes], 0)
                        .expect("sending result on results_interchange_to_submit");
                    slot_queue
                        .add(Slot {
                            manager_id: manager_id.clone(),
                        })
                        .expect("enqueuing slot on result");
                } else {
                    panic!("Unknown result-like message part type");
                }
            }
        }

        // socket monitoring
        if sockets[4].get_revents().contains(zmq::PollEvents::POLLIN) {
            let parts = zmq_tasks_submit_to_interchange_monitor
                .recv_multipart(0)
                .expect("monitoring message");
            println!("got a zmq monitoring message for zmq_tasks_submit_to_interchange_monitor");
            assert!(parts.len() == 2);
            let a = &parts[0];
            assert!(a.len() == 6);
            let mut a_reader = std::io::Cursor::new(a);
            let a_event_type = a_reader
                .read_u16::<byteorder::NativeEndian>()
                .expect("Parsing event type");

            let a_event_info = decode_zmq_monitor_event(a_event_type);

            let b = &parts[1];
            // is this UTF-8? maybe not? ZMQ docs a bit unclear TODO
            let b_endpoint = std::str::from_utf8(b).expect("UTF-8(?) encoded endpoint");

            println!(
                "ZMQ Monitoring: event type {} ({}), distant endpoint: {}",
                a_event_type, a_event_info, b_endpoint
            );
        }

        // we've maybe added tasks and slots to the slot queues, so now
        // do some match-making. This only needs to happen if both queues
        // are non-empty:

        while task_queue.size() > 0 && slot_queue.size() > 0 {
            println!("matching a slot and a task");
            let task = task_queue
                .remove()
                .expect("reasoning violation: task_queue is non-empty, by while condition");
            let slot = slot_queue
                .remove()
                .expect("reasoning violation: slot_queue is non-empty, by while condition");

            let empty: [u8; 0] = []; // see protocol description for this weird unnecessary(TODO?) field

            let task_list = serde_pickle::value::Value::List(
                [serde_pickle::value::Value::Dict(
                    std::collections::BTreeMap::from([
                        (
                            serde_pickle::value::HashableValue::String("task_id".to_string()),
                            serde_pickle::value::Value::I64(task.task_id),
                        ),
                        (
                            serde_pickle::value::HashableValue::String("buffer".to_string()),
                            serde_pickle::value::Value::Bytes(task.buffer),
                        ),
                    ]),
                )]
                .to_vec(),
            );

            let task_list_pkl =
                serde_pickle::ser::value_to_vec(&task_list, serde_pickle::ser::SerOptions::new())
                    .expect("pickling tasks for workers");

            // now we send the task to the slot...
            let multipart_msg = [slot.manager_id, empty.to_vec(), task_list_pkl];
            zmq_tasks_interchange_to_workers
                .send_multipart(multipart_msg, 0)
                .expect("sending task to pool");
        }
    }
}


// TODO: it would be nice if these could be autodecoded...
// it's a bit horrible transcribing symbol names between universes...
fn decode_zmq_monitor_event(a_event_type: u16) -> String {
    match a_event_type.into() {
        // this is a transcription of some event types from:
        // https://docs.rs/zmq-sys/latest/src/zmq_sys/ffi.rs.html#141
        // When you get an UNKNOWN, look it up there and add here.
        zmq_sys::ZMQ_EVENT_CONNECTED => "CONNECTED",
        zmq_sys::ZMQ_EVENT_CONNECT_DELAYED => "CONNECT_DELAYED",
        zmq_sys::ZMQ_EVENT_CONNECT_RETRIED => "CONNECT_RETRIED",
        zmq_sys::ZMQ_EVENT_CLOSED => "CLOSED",
        zmq_sys::ZMQ_EVENT_DISCONNECTED => "DISCONNECTED",
        zmq_sys::ZMQ_EVENT_HANDSHAKE_SUCCEEDED => "HANDSHAKE_SUCCEEDED",

        zmq_sys::ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL => "HANDSHAKE_FAILED_PROTOCOL",
        // TODO: looks like if this happens, nothing is going to retry and the socket is dead
        // in which case we might as well panic the whole interchange
        // and then TODO have the executor notice the interchange died. Rather than hang
        // waiting for an interchange that will likely never connect? (that's part of
        // exit-protocol - another implicit comms channel from interchange to the
        // submit side)
        _ => panic!("Unknown monitoring event type {}", a_event_type), // panic to force development. would also be OK to return UNKNOWN
    }
    .to_string()
    // TODO can I use strs somehow? to return a &str, needs some lifetime work?
}



/* this will attempt to interpret a minimal ZPL format as generated by pyzmq in parsl enough
   to extract the keys, but no more.

   TODO: would be fun to learn about serde plugins by implementing this that way though, though?

   here's an example:

cat .pytest/parsltest-current/runinfo/000/htex_local/certificates/server.key_secret 
#   ****  Generated on 2024-04-19 12:26:33.192381 by pyzmq  ****
#   ZeroMQ CURVE **Secret** Certificate
#   DO NOT PROVIDE THIS FILE TO OTHER USERS nor change its permissions.

metadata
curve
    public-key = "eB1E<[&$<[b@1Wurj{WQsNiI>hjrVn5}Dd&P=5[/"
    secret-key = "NA[NSHUTXs/]KJmF2s]Nl]jYEi+Mo*a4Nl^sK!]@"

 */

fn load_zmq_keypair(keyfile: &std::path::Path) -> zmq::CurveKeyPair {
    let f = std::fs::File::open(keyfile).expect("opening key file");
    let mut lines = std::io::BufReader::new(f).lines();

    // scan till we find a 'curve' top level line
    // TODO: double .expect is a bit ugh
    while lines.next().expect("more lines, rather than EOF before keys").expect("no io error") != "curve" {
        println!("load_zmq_keypair: discarding line")
    }

    // we just read the curve line... so the next two lines should be the two keys...
    let public_key_line = lines.next().expect("iterator can iterate").expect("no io error");
    let secret_key_line = lines.next().expect("iterator can iterate").expect("no io error");

    // tokenise these lines by whitespace, so we get three tokens per line, name = and value
    let mut public_key_line_iter = public_key_line.split_whitespace();
    let pk_name = public_key_line_iter.next();
    assert!(pk_name == Some("public-key"));
    let pk_eq = public_key_line_iter.next();
    assert!(pk_eq == Some("="));
    let pk_val_str = public_key_line_iter.next().expect("quoted key value");
    let pk_val_str = &pk_val_str[1..41];
    assert!(pk_val_str.len() % 5 == 0); // basic validation of z85 string length, because docs suggest this bit doesn't happen?
    let pk = zmq::z85_decode(pk_val_str).expect("decode z85"); // TODO strip the quotes?
    assert!(pk.len() == 32);


    let mut secret_key_line_iter = secret_key_line.split_whitespace();
    let sk_name = secret_key_line_iter.next();
    assert!(sk_name == Some("secret-key"));
    let sk_eq = secret_key_line_iter.next();
    assert!(sk_eq == Some("="));
    let sk_val_str = secret_key_line_iter.next().expect("quoted key value");
    let sk_val_str = &sk_val_str[1..41];
    assert!(sk_val_str.len() % 5 == 0); // basic validation of z85 string length, because docs suggest this bit doesn't happen?
    let sk = zmq::z85_decode(sk_val_str).expect("decode z85"); // TODO strip the quotes?
    assert!(sk.len() == 32);

    zmq::CurveKeyPair {
        public_key: <[u8; 32]>::try_from(pk).expect("unpacking Vec"),
        secret_key: <[u8; 32]>::try_from(sk).expect("unpacking Vec")
    }
}
