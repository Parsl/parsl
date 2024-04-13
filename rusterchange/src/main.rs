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

    let zmq_tasks_submit_to_interchange_socket = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create task_submit_to_interchange socket");
    zmq_tasks_submit_to_interchange_socket
        .connect("tcp://127.0.0.1:9000")
        .expect("could not connect task_submit_to_interchange socket");

    let zmq_results_interchange_to_submit_socket = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create results_interchange_to_submit socket");
    zmq_results_interchange_to_submit_socket
        .connect("tcp://127.0.0.1:9001")
        .expect("could not connect results_interchange_to_submit socket");

    let zmq_command = zmq_ctx
        .socket(zmq::SocketType::DEALER)
        .expect("could not create command socket");
    zmq_command
        .connect("tcp://127.0.0.1:9002")
        .expect("could not connect command socket");

    // the bind addresses for these two ports need to be the self.interchange_address, not localhost
    // in order to accept connections from remote workers
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

        let mut sockets = [
            zmq_tasks_submit_to_interchange_socket.as_poll_item(zmq::PollEvents::POLLIN),
            // zmq_results_interchange_to_submit_socket.as_poll_item(zmq::PollEvents::all()),
            zmq_command.as_poll_item(zmq::PollEvents::POLLIN),
            // zmq_tasks_interchange_to_workers.as_poll_item(zmq::PollEvents::all()),
            zmq_results_workers_to_interchange.as_poll_item(zmq::PollEvents::POLLIN),
        ];

        println!("Polling");
        let count = zmq::poll(&mut sockets, -1).expect("poll failed");
        // -1 means infinite timeout, but maybe we should timeout on the heartbeat periods
        // in order to send and receive heartbeats? TODO

        println!("Found {} interesting socket(s)", count);

        // tasks submit to interchange channel

        println!(
            "tasks submit to interchange events: {}",
            sockets[0].get_revents().bits()
        );
        if sockets[0].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("Poll result: there is a task from the submit side");
            let task = zmq_tasks_submit_to_interchange_socket
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
        if sockets[0].get_revents().contains(zmq::PollEvents::POLLOUT) {
            println!("poll out"); // we can write to this socket. actually probably should avoid polling for this? or perhaps optionally if we want to send a heartbeat without blocking?
        }
    }
}
