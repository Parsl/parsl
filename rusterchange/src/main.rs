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

    let zmq_tasks_submit_to_interchange_socket = zmq_ctx.socket(zmq::SocketType::DEALER).expect("could not create task_submit_to_interchange socket");
    zmq_tasks_submit_to_interchange_socket.connect("tcp://127.0.0.1:9000").expect("could not connect task_submit_to_interchange socket");

    let zmq_results_interchange_to_submit_socket = zmq_ctx.socket(zmq::SocketType::DEALER).expect("could not create results_interchange_to_submit socket");
    zmq_results_interchange_to_submit_socket.connect("tcp://127.0.0.1:9001").expect("could not connect results_interchange_to_submit socket");

    let zmq_command = zmq_ctx.socket(zmq::SocketType::DEALER).expect("could not create command socket");
    zmq_command.connect("tcp://127.0.0.1:9002").expect("could not connect command socket");


    // the bind addresses for these two ports need to be the self.interchange_address, not localhost
    // in order to accept connections from remote workers
    let zmq_tasks_interchange_to_workers = zmq_ctx.socket(zmq::SocketType::ROUTER).expect("could not create tasks_interchange_to_workers socket");
    zmq_tasks_interchange_to_workers.bind("tcp://127.0.0.1:9003").expect("could not bind tasks_interchange_to_workers");

    let zmq_results_workers_to_interchange = zmq_ctx.socket(zmq::SocketType::ROUTER).expect("could not create results_workers_to_interchange socket");
    zmq_results_workers_to_interchange.bind("tcp://127.0.0.1:9004").expect("could not bind results_workers_to_interchange");

    loop {

        // unclear to me what it means to share this sockets list across multiple loop iterations?

        // choose what to poll for based on if we want to be able to send things (in which case
        // poll for POLLOUT) otherwise we don't care about POLLOUT... I'm a bit unclear how much
        // data can be written when we've got a POLLOUT?

        let mut sockets = [zmq_tasks_submit_to_interchange_socket.as_poll_item(zmq::PollEvents::POLLIN),
                           // zmq_results_interchange_to_submit_socket.as_poll_item(zmq::PollEvents::all()),
                           zmq_command.as_poll_item(zmq::PollEvents::POLLIN),
                           // zmq_tasks_interchange_to_workers.as_poll_item(zmq::PollEvents::all()),
                           zmq_results_workers_to_interchange.as_poll_item(zmq::PollEvents::POLLIN)
                          ];

        println!("Polling");
        let count = zmq::poll(&mut sockets, -1).expect("poll failed");
        // -1 means infinite timeout, but maybe we should timeout on the heartbeat periods
        // in order to send and receive heartbeats? TODO
                    
        println!("Found {} interesting socket(s)", count);

        // tasks submit to interchange channel

        println!("tasks submit to interchange events: {}", sockets[0].get_revents().bits());
        if sockets[0].get_revents().contains(zmq::PollEvents::POLLIN) {
            println!("Poll result: there is a task from the submit side");
            let task = zmq_tasks_submit_to_interchange_socket.recv_bytes(0).expect("reading task message");
            print!("Message: ");
            for b in task { 
                print!("{} ", b);
            }
            println!("");

        }

        // TODO: this isn't polled for, so should be impossible to be reached...
        // but there's no static verification of that...
        if sockets[0].get_revents().contains(zmq::PollEvents::POLLOUT) {
            println!("poll out"); // we can write to this socket. actually probably should avoid polling for this? or perhaps optionally if we want to send a heartbeat without blocking?
        }
    }
}
