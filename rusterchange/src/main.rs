use queues::IsQueue;

// Vocab:
// This document attempts to avoid the use of "client" and "server" as
// there are multiple interpretations for that even on a single connection,
// and htex is not a clear client server architecture.

// threadedness: single threaded as much as possible

// TODO: there's a multiprocessing message queue used at start-up that this interchange does not implement
// I should replace it with a PORTS command I think? In the prototype it's hacked out and only works with hard-coded ports.
// Removing multiprocessing fork and perhaps using a regular python fork/exec would force this to happen anyway?

// TODO: terminology needs clarifying and consistenifying: managers, pools, workers. are we sending things to/from a "pool" or "manager"? is the ID for a "manager" or for a "pool"?

// TODO: this code has no handling/reasoning about what happens when any ZMQ queue is unable to deal with a `send` call (aka its full)
// TOOD: this code has no handling/reasoning about what happens if a ZMQ socket doesn't actually connect: as I've seen both in real Python Parsl and while developing this implementation, there's a lot of silent hangs hoping things get better - ZMQ Monitoring Sockets might be interesting there.
// TODO: heartbeats are not implemented. there are a few heartbeats that are distinct, and perhaps it would be good to name them, "this is the heartbeat that detects XXX failing". heartbeats also need to pickle new python objects (the exception for missing task) which might be simple enough to do here as there isn't much in there?

// TODO: monitoring - both node table messages from the interchange and relaying on htex-radio messages from workers, over ZMQ

// TODO: in development, the interchange often panics and exits, and that leaves htex in a hung state, rather than noticing and failing the tests: the user equivalent of that is eg. if oom-killer kills the real Python interchange, a run will hang rather than report an error... everything else has heartbeats but not this... (or even process-aliveness-checking...)

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

    // we've got 5 communication channels - they're all ZMQ
    // three to the submit side (sometimes called the client side) - TCP outbound
    // two to the worker side - TCP listening

    // there's also a startup-time channel that is in master parsl a multiprocessing.Queue
    // that sends the port numbers for the two listening ports chosen at bind time.

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
    zmq_tasks_submit_to_interchange
        .connect("tcp://127.0.0.1:9000")
        .expect("could not connect task_submit_to_interchange socket");

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
    // This rust code probably doesn't implement all the commands - just as I
    // find my progress stopped by a missing command, I'll implement the next one.
    // Some commands are (as python pickled values) -- see _command_server in interchange.py
    //    "CONNECTED_BLOCKS"  -- return a List[str] connecting block IDs for every block that has connected. Blocks might be repeated (perhaps once per manager?)   TODO: that's probably a smell in the protocol: with thousands of nodes, this would make a 1-block message contain thousands of strings.
    let zmq_command = zmq_ctx
        .socket(zmq::SocketType::REP)
        .expect("could not create command socket");
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
    let zmq_tasks_interchange_to_workers = zmq_ctx
        .socket(zmq::SocketType::ROUTER)
        .expect("could not create tasks_interchange_to_workers socket");
    zmq_tasks_interchange_to_workers
        .bind("tcp://127.0.0.1:9003")
        .expect("could not bind tasks_interchange_to_workers");

    // In the workers to interchange direction, this carries results from tasks which were previously sent over the tasks_workers_to_interchange channel. Messges take the form of arbitrary length multipart messages. The first part as received from recv_multipart will be the manager ID (added by ZMQ because this is a ROUTER socket) and then each subsequent part will be a pickle containing a result. Note that this is different from the wrapping used on tasks_interchange_to_workers, where a single pickle object is sent, containing a pickle/python level list of task definitions. TODO: consistentify the multipart vs python list form.
    // The pickled object is a Python dictionary with a type entry that is one of these strings:  'result' 'monitoring' or 'heartbeat'.
    // The rest of the dictionary depends on that type.
    // TODO: this is a pickled dict, vs tasks_interchange_to_workers
    let zmq_results_workers_to_interchange = zmq_ctx
        .socket(zmq::SocketType::ROUTER)
        .expect("could not create results_workers_to_interchange socket");
    zmq_results_workers_to_interchange
        .bind("tcp://127.0.0.1:9004")
        .expect("could not bind results_workers_to_interchange");

    let mut task_queue: queues::Queue<Task> = queues::Queue::new();
    let mut slot_queue: queues::Queue<Slot> = queues::Queue::new();

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
            print!("Message: ");
            for b in &task {
                print!("{} ", b);
            }
            let t = serde_pickle::de::value_from_slice(&task, serde_pickle::de::DeOptions::new())
                .expect("unpickling");
            println!("Unpickled: {}", t);
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
                task_id: *task_id,
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
            println!("Message from workers to interchange: {}", json); // TODO: log the manager ID too...
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
                }; // TODO: should I add on prefetch here? (or is that included in max_workers by the pool?)
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
            } else {
                panic!("Cannot handle command")
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

            let empty: [u8; 0] = []; // see protocol description for this weird unnecessary(?) field

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
