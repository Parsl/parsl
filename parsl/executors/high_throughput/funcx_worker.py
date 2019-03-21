import logging
import argparse
import zmq
import os
from ipyparallel.serialize import serialize_object, unpack_apply_message

def funcx_worker(worker_id, pool_id, task_url, result_url, logdir, debug=True):
    """

    Funcx worker will use the REP sockets to:
         task = recv ()
         result = execute(task)
         send(result)
    """


    start_file_logger('{}/{}/funcx_worker_{}.log'.format(logdir, pool_id, worker_id),
                      worker_id,
                      name="worker_log",
                      level=logging.DEBUG if debug else logging.INFO)

    try:
        # Later check if filepath cannot exist on filesystem.
        os.makedirs("{}/{}".format(logdir, pool_id))
        print("MADE FILE")
    except:
        logger.debug("ERROR MAKING DIRS AT {}/{}".format(logdir,pool_id))
        pass

    # Sync worker with master
    logger.info('Worker {} started'.format(worker_id))
    if debug:
        logger.debug("Debug logging enabled")

    context = zmq.Context()

    funcx_worker_socket = context.socket(zmq.REP)
    funcx_worker_socket.connect(task_url)
    logger.debug("Connecting to {}".format(task_url))

    while True:
        logger.debug("IN TASK-RECEIVE LOOP")
        # This task receiver socket is blocking.
        b_task_id, *buf = funcx_worker_socket.recv_multipart()
        # msg = task_socket.recv_pyobj()
        logger.debug("Got buffer : {}".format(buf))

        try:
            task_id = int.from_bytes(b_task_id, "little")

            logger.info("Received task {}".format(task_id))
        except Exception as e:
            logger.debug("CAUGHT THIS: {}".format(e))


        # serialized_result = serialize_object(1)
        # result_package = {'task_id': task_id, 'result': serialized_result}

        try:
            logger.debug("Moving to execute function...")
            result = execute_task(buf)

            logger.debug("Successfully executed task!")
            serialized_result = serialize_object(result)
        except Exception:
            result_package = {'task_id': task_id, 'exception': serialize_object(RemoteExceptionWrapper(*sys.exc_info()))}
            logger.debug("Got exception something")
        else:
            result_package = {'task_id': task_id, 'result': serialized_result}
            logger.debug("DID WE COMPLETE EXECUTION?") 

        logger.info("Completed task {}".format(task_id))
        pkl_package = pickle.dumps(result_package)

        funcx_worker_socket.send_multipart([pkl_package])


def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """

    logger.debug("Inside execute_task function")
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    logger.debug("Message unpacked")

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    logger.debug("Namespace updated")

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    logger.debug("Code string created: {}".format(code))
    try:
        # logger.debug("[RUNNER] Executing: {0}".format(code))
        logger.debug("Arguments parsed. Executing!!")
        exec(code, user_ns, user_ns)
        logger.debug("Successfully executed!")

    except Exception as e:
        # logger.warning("Caught exception; will raise it: {}".format(e), exc_info=True)
        raise e

    else:
        # logger.debug("[RUNNER] Result: {0}".format(user_ns.get(resultname)))
        return user_ns.get(resultname)


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(rank)

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker_id")
    parser.add_argument("--pool_id")
    parser.add_argument("--task_url")
    parser.add_argument("--result_url")
    parser.add_argument("--logdir")

    args = parser.parse_args()


    worker = funcx_worker(args.worker_id, args.pool_id, args.task_url, args.result_url, args.logdir)
