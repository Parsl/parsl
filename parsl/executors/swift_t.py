''' Sample Executor for integration with SwiftT.

This follows the model used by `EMEWS <http://www.mcs.anl.gov/~wozniak/papers/Cancer2_2016.pdf>`_ to some extent.

'''
import concurrent.futures
from concurrent.futures import Future
import logging
import uuid
import threading
import weakref
import time
import sys, errno
import queue
from queue import Queue
import multiprocessing as mp

from ipyparallel.serialize import pack_apply_message, unpack_apply_message
from ipyparallel.serialize import serialize_object, deserialize_object

import parsl
from parsl.executors.base import ParslExecutor

logger = logging.getLogger(__name__)


BUFFER_THRESHOLD = 1024*1024
ITEM_THRESHOLD = 1024


def runner(incoming_q, outgoing_q):
    ''' This is a function that mocks the Swift-T side. It listens on the the incoming_q for tasks
    and posts returns on the outgoing_q

    Args:
         - incoming_q (Queue object) : The queue to listen on
         - outgoing_q (Queue object) : Queue to post results on

    The messages posted on the incoming_q will be of the form :

    {
      "task_id" : <uuid.uuid4 string>,
      "buffer"  : serialized buffer containing the fn, args and kwargs
    }

    If ``None`` is received, the runner will exit.

    Response messages should be of the form:

    {
      "task_id" : <uuid.uuid4 string>,
      "result"  : serialized buffer containing result
      "exception" : serialized exception object
    }

    On exiting the runner will post ``None`` to the outgoing_q

    '''
    logger.debug("[RUNNER] Starting")

    def execute_task(bufs):
        ''' Deserialize the buf, and execute the task.
        Returns the serialized result/exception
        '''
        all_names = dir(__builtins__)
        user_ns   = locals()
        user_ns.update( {'__builtins__' : {k : getattr(__builtins__, k)  for k in all_names} } )

        f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

        fname = getattr(f, '__name__', 'f')
        prefix     = "parsl_"
        fname      = prefix+"f"
        argname    = prefix+"args"
        kwargname  = prefix+"kwargs"
        resultname = prefix+"result"

        user_ns.update({ fname : f,
                         argname : args,
                         kwargname : kwargs,
                         resultname : resultname })

        code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                               argname, kwargname)

        try:

            print("[RUNNER] Executing : {0}".format(code))
            exec(code, user_ns, user_ns)

        except Exception as e:
            logger.warn("Caught errors but will not handled %s", e)
            raise e

        else :
            #print("Done : {0}".format(locals()))
            print("[RUNNER] Result    : {0}".format(user_ns.get(resultname)))
            return user_ns.get(resultname)


    while True :
        try:
            # Blocking wait on the queue
            msg = incoming_q.get(block=True, timeout=10)
            #logger.debug("[RUNNER] Got message : %s", msg)

        except queue.Empty as e:
            # Handle case where no items were on queue
            logger.debug("[RUNNER] got nothing")
            pass

        except IOError as e:
            logger.debug("[RUNNER] broken pipe, error: %s", e)
            try:
                # Attempt to send a stop notification to the management thread
                outgoing_q.put(None)
            except Exception as e:
                pass
            break

        except Exception as e:
            logger.debug("[RUNNER] caught unknown exception : %s", e)
            pass

        else:
            # Handle received message
            if not msg :
                # Empty message is a die request
                logger.debug("[RUNNER] Received exit request")
                outgoing_q.put(None)
                break
            else:
                # Received a valid message, handle it
                logger.debug("[RUNNER] Got a valid task : %s", msg["task_id"])
                try:
                    response_obj = execute_task(msg['buffer'])
                    response = {"task_id" : msg["task_id"],
                                "result"  : serialize_object(response_obj)}

                    logger.warn("[RUNNER] Returing result : %s",
                                deserialize_object(response["result"]) )

                except Exception as e:
                    logger.debug("[RUNNER] Caught task exception")
                    response = {"task_id" : msg["task_id"],
                                "exception"  : serialize_object(e)}

                outgoing_q.put(response)

    logger.debug("[RUNNER] Terminating")


class TurbineExecutor(ParslExecutor):
    ''' The Turbine executor. Bypass the Swift/T language and run on top off the Turbine engines
    in an MPI environment.

    Here's a simple diagram

                 |  Data   |  Executor   |   IPC      | External Process(es)
                 |  Flow   |             |            |
            Task | Kernel  |             |            |
          +----->|-------->|------------>|Outgoing_Q -|-> Worker_Process
          |      |         |             |            |    |         |
    Parsl<---Fut-|         |             |            |  result   exception
              ^  |         |             |            |    |         |
              |  |         |   Q_mngmnt  |            |    V         V
              |  |         |    Thread<--|Incoming_Q<-|--- +---------+
              |  |         |      |      |            |
              |  |         |      |      |            |
              +----update_fut-----+

    '''

    def _queue_management_worker(self):
        ''' The queue management worker is responsible for listening to the incoming_q
        for task status messages and updating tasks with results/exceptions/updates

        It expects the following messages:
        {
           "task_id" : <task_id>
           "result"  : serialized result object, if task succeeded
           ... more tags could be added later
        }

        {
           "task_id" : <task_id>
           "exception" : serialized exception object, on failure
        }

        We don't support these yet, but they could be added easily as heartbeat.

        {
           "task_id" : <task_id>
           "cpu_stat" : <>
           "mem_stat" : <>
           "io_stat"  : <>
           "started"  : tstamp
        }

        The None message is a die request.
        None

        '''
        while True:
            logger.debug("[MTHREAD] Management thread active")
            try:
                msg = self.Incoming_Q.get(block=True, timeout=1)

            except queue.Empty as e:
                # timed out.
                pass

            except IOError as e:
                logger.debug("[MTHREAD] caught broken queue : %s : errno:%s", e, e.errno)
                return

            except Exception as e:
                logger.debug("[MTHREAD] caught unknown exception : %s", e)
                pass

            else:
                if msg == None:
                    logger.debug("[MTHREAD] Got None")
                    return
                else:
                    logger.debug("[MTHREAD] Got message : %s", msg)
                    task_fut = self.tasks[msg['task_id']]
                    if 'result' in msg:
                        result, remainder = deserialize_object(msg['result'])
                        task_fut.set_result(result)

                    elif 'exception' in msg:
                        exception, remainder = deserialize_object(msg['exception'])
                        task_fut.set_exception(exception)

            if not self.isAlive:
                break

    # When the executor gets lost, the weakref callback will wake up
    # the queue management thread.
    def weakref_cb(_, q=None):
        ''' We do not use this yet
        '''

        q.put(None)

    def _start_queue_management_thread(self):
        ''' Method to start the management thread as a daemon.
        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the management thread dies.
        '''

        logging.debug("In _start %s", "*"*40)
        if self._queue_management_thread == None:
            logging.debug("Starting management thread ")
            self._queue_management_thread = threading.Thread (target=self._queue_management_worker)
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
        else:
            logging.debug("Management thread already exists, returning")


    def shutdown(self):
        ''' Shutdown method, to kill the threads and workers.
        '''

        self.isAlive = False
        logging.debug("Waking management thread")
        self.Incoming_Q.put(None) # Wake up the thread
        self._queue_management_thread.join() # Force join
        logging.debug("Exiting thread")
        self.worker.join()
        return True

    def __init__ (self, max_workers=2, thread_name_prefix=''):
        ''' Initialize the thread pool
        Trying to implement the emews model.

        '''
        logger.debug("In __init__")
        self.mp_manager = mp.Manager()
        self.Outgoing_Q = self.mp_manager.Queue()
        self.Incoming_Q = self.mp_manager.Queue()
        self.isAlive   = True

        self._queue_management_thread = None
        self._start_queue_management_thread()
        logger.debug("Created management thread : %s", self._queue_management_thread)

        self.worker  = mp.Process(target=runner, args = (self.Outgoing_Q, self.Incoming_Q))
        self.worker.start()
        logger.debug("Created worker : %s", self.worker)
        self.tasks   = {}

    def submit (self, func, *args, **kwargs):
        ''' Submits work to the thread pool
        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        '''
        task_id = uuid.uuid4()

        logger.debug("Before pushing to queue : func:%s func_args:%s", func, args)

        self.tasks[task_id] = Future()

        fn_buf  = pack_apply_message(func, args, kwargs,
                                     buffer_threshold=1024*1024,
                                     item_threshold=1024)

        msg = {"task_id" : task_id,
               "buffer"  : fn_buf }

        # Post task to the the outgoing queue
        self.Outgoing_Q.put(msg)

        # Return the future
        return self.tasks[task_id]


    def scale_out (self, workers=1):
        ''' Scales out the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.
        This would be nice to have, and can be done

        Raises:
             NotImplemented exception
        '''

        raise NotImplemented

    def scale_in (self, workers=1):
        ''' Scale in the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''

        raise NotImplemented




if __name__ == "__main__" :

    print("Start")
    tex = TurbineExecutor()
    print("Done")
