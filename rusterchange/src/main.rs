use queues::IsQueue;

// use of Queue wants Clone trait, which is a bit suspicious: does that mean we have
// explicit clones of the (potentially large) buffer in interchange? when ideally we
// would pass around the buffer linearly without ever duplicating it? TODO
#[derive(Clone)]
struct Task {
    task_id: i64,
    buffer: Vec<u8>,
}

fn main() {
    println!("the rusterchange");

    // we've got 5 communication channels - they're all ZMQ
    // three to the submit side (sometimes called the client side) - TCP outbound
    // two to the worker side - TCP listening

    // there's also a startup-time channel that is in master parsl a multiprocessing.queue
    // that sends the port numbers for the two listening ports.

    // ... and we shutdown on unix termination signals, not on a message.

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
    // a single interchange.
    let zmq_tasks_submit_to_interchange = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create task_submit_to_interchange socket");
    zmq_tasks_submit_to_interchange
        .connect("tcp://127.0.0.1:9000")
        .expect("could not connect task_submit_to_interchange socket");

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
    // In the interchange to worker direction: TODO format notes
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

    let zmq_results_workers_to_interchange = zmq_ctx
        .socket(zmq::SocketType::ROUTER)
        .expect("could not create results_workers_to_interchange socket");
    zmq_results_workers_to_interchange
        .bind("tcp://127.0.0.1:9004")
        .expect("could not bind results_workers_to_interchange");

    let mut task_queue: queues::Queue<Task> = queues::Queue::new();

    loop {
        // unclear to me what it means to share this sockets list across multiple loop iterations?

        // choose what to poll for based on if we want to be able to send things (in which case
        // poll for POLLOUT) otherwise we don't care about POLLOUT... I'm a bit unclear how much
        // data can be written when we've got a POLLOUT?

        // alas because of move semantics, these are not re-usable...
        let zmq_tasks_submit_to_interchange_poll_item = zmq_tasks_submit_to_interchange.as_poll_item(zmq::PollEvents::POLLIN);
        let zmq_tasks_interchange_to_workers_poll_item = zmq_tasks_interchange_to_workers.as_poll_item(zmq::PollEvents::POLLIN); // see protocol description for why we should be POLLIN polling on what sounds like its a send-only channel
        let mut sockets = [
            zmq_tasks_submit_to_interchange_poll_item,
            zmq_tasks_interchange_to_workers_poll_item,
            zmq_command.as_poll_item(zmq::PollEvents::POLLIN)
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
            println!("");
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

            // now there's two options: either we can match against capacity of a registered manager
            // or if there is no capacity, we can queue this and match when a manager indicates
            // it has capacity.

            // manager capacity isn't implemented... so all we can do is queue it here.
            // perhaps consider that i'll want to do richer matchmaking based on resource stuff contained in protocol (see outreachy internship) and be prepared to use something richer than a queue?

            let task = Task {
                task_id: *task_id,
                buffer: buffer.clone(), // TODO: awkward clone here of buffer but I guess because of serde_pickle, we have to clone it out of the task_dict value if we're doing shared values... perhaps there is a way to convert the task dict into the buffer forgetting everything else, linearly? TODO
            };
            task_queue.add(task).expect("queue broken - eg full?");
        }

        // TODO: this isn't polled for, so should be impossible to be reached...
        // but there's no static verification of that...
        if sockets[1].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("reverse message on tasks_interchange_to_workers");
            // this is JSON, not pickle
            let message = zmq_tasks_interchange_to_workers
                         .recv_multipart(0)
                         .expect("reading worker message from tasks_submit_to_interchange channel");
            let manager_id = &message[0];
            let json_bytes = &message[1];
            let json: serde_json::Value = serde_json::from_slice(json_bytes).expect("protocol error");
            println!("Message from workers to interchange: {}", json); // TODO: log the manager ID too...
        }

        if sockets[2].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("command received from submit side");
            // this a REQ/REP pair, with this end being a REP, so we MUST
            // send back a single response message.
            let cmd_pickle_bytes = zmq_command
                                   .recv_bytes(0)
                                   .expect("reading command message");
            let cmd = serde_pickle::de::value_from_slice(&cmd_pickle_bytes, serde_pickle::de::DeOptions::new())
                      .expect("unpickling");
            println!("Unpickled command: {}", cmd);
            let resp_pkl = if cmd == serde_pickle::Value::String("CONNECTED_BLOCKS".to_string()) {
                // TODO: this needs to return all blocks that have ever had a manager connect,
                // even for blocks that no longer have a manager connected.
                serde_pickle::ser::value_to_vec(&serde_pickle::value::Value::List([].to_vec()), serde_pickle::ser::SerOptions::new()).expect("pickling block list")
            } else {
                panic!("Cannot handle command")
            };
            zmq_command.send(resp_pkl, 0).expect("sending command response");
        }
    }
}
