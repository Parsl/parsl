import time
import logging
import argparse

import zmq
from multiprocessing import Process, Queue

from ipyparallel.serialize import pack_apply_message, unpack_apply_message
from ipyparallel.serialize import deserialize_object, serialize_object


logger = logging.getLogger(__name__)


def double(x):
    return 2*x


def execute_task(f, args, kwargs, user_ns):
    """Deserialize the buffer and execute the task.

    Returns the result or exception.
    """
    fname = getattr(f, '__name__', 'f')
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)
    try:
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e))
        raise e

    else:
        return user_ns.get(resultname)


def simple_executor(num_tasks=10000):
    serialization_times = []
    exec_times = []
    results = []

    for i in range(num_tasks):
        task_id = i
        start_time = time.time()
        buf = pack_apply_message(f=double, args=[i], 
                                 kwargs={"task_id": task_id},
                                 buffer_threshold=1024 * 1024,
                                 item_threshold=1024)
        serialization_times.append(time.time() - start_time)

        start_time = time.time()
        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(buf, user_ns, copy=False)
        task_id = kwargs["task_id"]
        del kwargs["task_id"]
        result = execute_task(f, args, kwargs, user_ns)
        exec_times.append(time.time() - start_time)
        
        results.append(result)
    
    print("[WITHOUT-ZEROMQ] Avg serialization time: \t {:=10.4f} us"
          .format(10 ** 6 * sum(serialization_times) / len(serialization_times)))
    print("[WITHOUT-ZEROMQ] Avg execution time: \t\t {:=10.4f} us"
          .format(10 ** 6 * sum(exec_times) / len(exec_times)))
    
    return results


def dealer_execute_task(worker_id, port=5560):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://localhost:{}".format(port))
    logger.info("Starting worker {}".format(worker_id))

    while True:
        bufs = socket.recv_multipart()

        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)
        task_id = kwargs["task_id"]
        del kwargs["task_id"]

        logger.debug("Worker {} received message {}".format(worker_id, task_id))
        result = execute_task(f, args, kwargs, user_ns)
        logger.debug("Worker result: {}".format(result))
        reply = {"result": result, "task_id": task_id}
        socket.send_multipart(serialize_object(reply))


def dealer_interchange(manager_port=5559, worker_port=5560):
    context = zmq.Context()
    incoming = context.socket(zmq.ROUTER)
    outgoing = context.socket(zmq.DEALER)

    incoming.bind("tcp://*:{}".format(manager_port))
    outgoing.bind("tcp://*:{}".format(worker_port))

    poller = zmq.Poller()
    poller.register(incoming, zmq.POLLIN)
    poller.register(outgoing, zmq.POLLIN)

    while True:
        socks = dict(poller.poll(1))

        if socks.get(incoming) == zmq.POLLIN:
            message = incoming.recv_multipart()
            logger.debug("[interchange] New task {}".format(message))
            outgoing.send_multipart(message)
    
        if socks.get(outgoing) == zmq.POLLIN:
            message = outgoing.recv_multipart()
            logger.debug("[interchange] New Result {}".format(message))
            incoming.send_multipart(message)


def dealer_executor(num_tasks=10000, port=5559, interchange=True):
    logger.info("Starting executor")
    label = "DEALER-INTERCHANGE-REP" if interchange else "DEALER-REP"

    serialization_times = []
    deserialization_times = []
    send_times = {}
    exec_times = {}
    results = []

    context = zmq.Context()
    dealer = context.socket(zmq.DEALER)
    if interchange:
        dealer.connect("tcp://localhost:{}".format(port))
    else:
        dealer.bind("tcp://*:{}".format(port))

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)

    num_send = 0
    num_recv = 0

    while True:
        socks = dict(poller.poll(1))
        if num_send < num_tasks:
            task_id = num_send
            start_time = time.time()
            buf = pack_apply_message(f=double, args=[num_send], 
                                     kwargs={"task_id": task_id},
                                     buffer_threshold=1024 * 1024,
                                     item_threshold=1024)
            serialization_times.append(time.time() - start_time)

            logger.debug("Manager sending task {}".format(task_id))
            send_times[task_id] = time.time()
            dealer.send_multipart([b""] + buf)
            num_send += 1
        
        if dealer in socks and socks[dealer] == zmq.POLLIN:
            buf = dealer.recv_multipart()
            exec_times[task_id] = time.time() - send_times[task_id]

            start_time = time.time()
            msg = deserialize_object(buf[1:])[0]
            deserialization_times.append(time.time() - start_time)
            
            logger.debug("Got message {}".format(msg))
            task_id = msg["task_id"]
            results.append(msg["result"])

            num_recv += 1
            logger.debug("Dealer received result {}".format(task_id))
            if num_recv == num_tasks:
                break

    print("[{}] Avg serialization time: \t {:=10.4f} us"
          .format(label, 
                  10 ** 6 * sum(serialization_times) / len(serialization_times)))
    # print("[{}] Avg deserialization time: {:=10.4f}"
        #   .format(label, 
        #           10 ** 6 * sum(deserialization_times) / len(deserialization_times)))
    print("[{}] Avg execution time: \t {:=10.4f} us"
          .format(label, 
                  10 ** 6 * sum(exec_times.values()) / len(exec_times)))
    
    return results
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-tasks", default=10000, type=int,
                        help="Number of tasks to send for benchmark")
    parser.add_argument("--num-workers", default=1, type=int,
                        help="Number of workers to use for Dealer-Rep")
    args = parser.parse_args()

    # Using Dealer-Reply without Interchange
    manager = Process(target=dealer_executor, 
                      kwargs={"num_tasks": args.num_tasks, "port": 5560, 
                              "interchange": False})
    manager.start()
    workers = []
    for i in range(args.num_workers):
        worker = Process(target=dealer_execute_task, 
                         kwargs={"worker_id": i, "port": 5560})
        worker.daemon = True
        worker.start()
        workers.append(worker)
    
    manager.join()
    for worker in workers:
        worker.terminate()
    
    # Using Dealer-Reply with Interchange
    manager = Process(target=dealer_executor, 
                      kwargs={"num_tasks": args.num_tasks, "port": 5559,
                              "interchange": True})
    manager.start()
    interchange = Process(target=dealer_interchange,
                          kwargs={"manager_port": 5559, "worker_port": 5560})
    interchange.daemon = True
    interchange.start()
    workers = []
    for i in range(args.num_workers):
        worker = Process(target=dealer_execute_task, 
                         kwargs={"worker_id": i, "port": 5560})
        worker.daemon = True
        worker.start()
        workers.append(worker)
    
    manager.join()
    interchange.terminate()
    for worker in workers:
        worker.terminate()

    # Naive Vanilla Version
    simple_executor(args.num_tasks)
