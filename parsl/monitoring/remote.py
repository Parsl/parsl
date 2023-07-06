import os
import time
import logging
import datetime
from functools import wraps

from parsl.multiprocessing import ForkProcess
from multiprocessing import Event, Queue
from queue import Empty
from parsl.process_loggers import wrap_with_logs

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios import MonitoringRadio, UDPRadio, ResultsRadio, HTEXRadio, FilesystemRadio
from typing import Any, Callable, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)

monitoring_wrapper_cache: Dict
monitoring_wrapper_cache = {}


def monitor_wrapper(f: Any,           # per app
                    args: Sequence,   # per invocation
                    kwargs: Dict,     # per invocation
                    x_try_id: int,    # per invocation
                    x_task_id: int,   # per invocation
                    monitoring_hub_url: str,   # per workflow
                    run_id: str,      # per workflow
                    logging_level: int,  # per workflow
                    sleep_dur: float,  # per workflow
                    radio_mode: str,   # per executor
                    monitor_resources: bool,  # per workflow
                    run_dir: str) -> Tuple[Callable, Sequence, Dict]:
    """Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
    """

    # this makes assumptions that when subsequently executed with the same
    # cache key, then the relevant parameters will not have changed from the
    # first invocation with that cache key (otherwise, the resulting cached
    # closure will be incorrectly cached)
    cache_key = (run_id, f, radio_mode)

    if cache_key in monitoring_wrapper_cache:
        wrapped = monitoring_wrapper_cache[cache_key]

    else:

        @wraps(f)
        def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            task_id = kwargs.pop('_parsl_monitoring_task_id')
            try_id = kwargs.pop('_parsl_monitoring_try_id')
            terminate_event = Event()
            terminate_queue: Queue[List[Any]]
            terminate_queue = Queue()
            # Send first message to monitoring router
            send_first_message(try_id,
                               task_id,
                               monitoring_hub_url,
                               run_id,
                               radio_mode,
                               run_dir)

            if monitor_resources:
                # create the monitor process and start
                # TODO: this process will make its own monitoring radio
                # which in the case of the ResultsRadio, at present will
                # not be able to get its results into this processes
                # monitoring messages list.
                # can I extract them right before kill time?
                pp = ForkProcess(target=monitor,
                                 args=(os.getpid(),
                                       try_id,
                                       task_id,
                                       monitoring_hub_url,
                                       run_id,
                                       radio_mode,
                                       logging_level,
                                       sleep_dur,
                                       run_dir,
                                       terminate_event,
                                       terminate_queue),
                                 daemon=True,
                                 name="Monitor-Wrapper-{}".format(task_id))
                pp.start()
                p = pp
                #  TODO: awkwardness because ForkProcess is not directly a constructor
                # and type-checking is expecting p to be optional and cannot
                # narrow down the type of p in this block.

            else:
                p = None

            # this logic flow is fairly contorted - can it look cleaner?
            # different wrapper structure, eg?
            try:
                ret_v = f(*args, **kwargs)
            finally:
                # There's a chance of zombification if the workers are killed by some signals (?)
                if p:
                    # TODO: can I get monitoring results out of here somehow?
                    # eg a shared object that comes back with more results?
                    # (terminate_event is already a shared object...)
                    # so just a single box that will be populated once at exit.
                    # nothing more nuanced than that - deliberately avoiding queues that can get full, for example.
                    terminate_event.set()
                    try:
                        more_monitoring_messages = terminate_queue.get(timeout=30)
                    except Empty:
                        more_monitoring_messages = []

                    p.join(30)  # 60 second delay for this all together (30+10) -- this timeout will be hit in the case of an unusually long end-of-loop
                    if p.exitcode is None:
                        logger.warn("Event-based termination of monitoring helper took too long. Using process-based termination.")
                        p.terminate()
                        # DANGER: this can corrupt shared queues according to docs.
                        # So, better that the above termination event worked.
                        # This is why this log message is a warning
                        p.join()

                send_last_message(try_id,
                                  task_id,
                                  monitoring_hub_url,
                                  run_id,
                                  radio_mode, run_dir)

            # if we reach here, the finally block has run, and
            # ret_v has been populated. so we can do the return
            # that used to live inside the try: block.
            # If that block raised an exception, then the finally
            # block would run, but then we would not come to this
            # return statement. As before.
            if radio_mode == "results":
                # this import has to happen here, not at the top level: we
                # want the result_radio_queue from the import on the
                # execution side - we *don't* want to get the (empty)
                # result_radio_queue on the submit side, send that with the
                # closure, and then send it (still empty) back. This is pretty
                # subtle, which suggests it needs either lots of documentation
                # or perhaps something nicer than using globals like this?
                from parsl.monitoring.radios import result_radio_queue
                assert isinstance(result_radio_queue, list)
                assert isinstance(more_monitoring_messages, list)

                full = result_radio_queue + more_monitoring_messages

                # due to fork/join when there are already results in the
                # queue, messages may appear in `full` via two routes:
                # once in process, and once via forking and joining.
                # At present that seems to happen only with first_msg messages,
                # so here check that full only has one.
                first_msg = [m for m in full if m[1]['first_msg']]  # type: ignore
                not_first_msg = [m for m in full if not m[1]['first_msg']]  # type: ignore

                # now assume there will be at least one first_msg
                full = [first_msg[0]] + not_first_msg

                return (full, ret_v)
            else:
                return ret_v

        monitoring_wrapper_cache[cache_key] = wrapped

    new_kwargs = kwargs.copy()
    new_kwargs['_parsl_monitoring_task_id'] = x_task_id
    new_kwargs['_parsl_monitoring_try_id'] = x_try_id

    return (wrapped, args, new_kwargs)


@wrap_with_logs
def send_first_message(try_id: int,
                       task_id: int,
                       monitoring_hub_url: str,
                       run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, False)


@wrap_with_logs
def send_last_message(try_id: int,
                      task_id: int,
                      monitoring_hub_url: str,
                      run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, True)


def send_first_last_message(try_id: int,
                            task_id: int,
                            monitoring_hub_url: str,
                            run_id: str, radio_mode: str, run_dir: str,
                            is_last: bool) -> None:
    import platform
    import os

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    elif radio_mode == "results":
        radio = ResultsRadio(monitoring_url=monitoring_hub_url,
                             source_id=task_id)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': not is_last,
            'last_msg': is_last,
            'timestamp': datetime.datetime.now()
    })
    radio.send(msg)
    return


@wrap_with_logs
def monitor(pid: int,
            try_id: int,
            task_id: int,
            monitoring_hub_url: str,
            run_id: str,
            radio_mode: str,
            logging_level: int,
            sleep_dur: float,
            run_dir: str,
            # removed all defaults because unused and there's no meaningful default for terminate_event.
            # these probably should become named arguments, with a *, and named at invocation.
            terminate_event: Any,
            terminate_queue: Any) -> None:  # cannot be Event because of multiprocessing type weirdness.
    """Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.

    This process makes calls to logging, but deliberately does not attach
    any log handlers. Previously, there was a handler which logged to a
    file in /tmp, but this was usually not useful or even accessible.
    In some circumstances, it might be useful to hack in a handler so the
    logger calls remain in place.
    """
    import logging
    import platform
    import psutil

    from parsl.utils import setproctitle

    setproctitle("parsl: task resource monitor")

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    elif radio_mode == "results":
        radio = ResultsRadio(monitoring_url=monitoring_hub_url,
                             source_id=task_id)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    logging.debug("start of monitor")

    # these values are simple to log. Other information is available in special formats such as memory below.
    simple = ["cpu_num", 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    # values that can be summed up to see total resources used by task process and its children
    summable_values = ['memory_percent', 'num_threads']

    pm = psutil.Process(pid)

    children_user_time = {}  # type: Dict[int, float]
    children_system_time = {}  # type: Dict[int, float]

    def accumulate_and_prepare() -> Dict[str, Any]:
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["run_id"] = run_id
        d["task_id"] = task_id
        d["try_id"] = try_id
        d['resource_monitoring_interval'] = sleep_dur
        d['hostname'] = platform.node()
        d['first_msg'] = False
        d['last_msg'] = False
        d['timestamp'] = datetime.datetime.now()

        logging.debug("getting children")
        children = pm.children(recursive=True)
        logging.debug("got children")

        d["psutil_cpu_count"] = psutil.cpu_count()
        d['psutil_process_memory_virtual'] = pm.memory_info().vms
        d['psutil_process_memory_resident'] = pm.memory_info().rss
        d['psutil_process_time_user'] = pm.cpu_times().user
        d['psutil_process_time_system'] = pm.cpu_times().system
        d['psutil_process_children_count'] = len(children)
        try:
            d['psutil_process_disk_write'] = pm.io_counters().write_chars
            d['psutil_process_disk_read'] = pm.io_counters().read_chars
        except Exception:
            # occasionally pid temp files that hold this information are unvailable to be read so set to zero
            logging.exception("Exception reading IO counters for main process. Recorded IO usage may be incomplete", exc_info=True)
            d['psutil_process_disk_write'] = 0
            d['psutil_process_disk_read'] = 0
        for child in children:
            for k, v in child.as_dict(attrs=summable_values).items():
                d['psutil_process_' + str(k)] += v
            child_user_time = child.cpu_times().user
            child_system_time = child.cpu_times().system
            children_user_time[child.pid] = child_user_time
            children_system_time[child.pid] = child_system_time
            d['psutil_process_memory_virtual'] += child.memory_info().vms
            d['psutil_process_memory_resident'] += child.memory_info().rss
            try:
                d['psutil_process_disk_write'] += child.io_counters().write_chars
                d['psutil_process_disk_read'] += child.io_counters().read_chars
            except Exception:
                # occassionally pid temp files that hold this information are unvailable to be read so add zero
                logging.exception("Exception reading IO counters for child {k}. Recorded IO usage may be incomplete".format(k=k), exc_info=True)
                d['psutil_process_disk_write'] += 0
                d['psutil_process_disk_read'] += 0
        total_children_user_time = 0.0
        for child_pid in children_user_time:
            total_children_user_time += children_user_time[child_pid]
        total_children_system_time = 0.0
        for child_pid in children_system_time:
            total_children_system_time += children_system_time[child_pid]
        d['psutil_process_time_user'] += total_children_user_time
        d['psutil_process_time_system'] += total_children_system_time
        logging.debug("sending message")
        return d

    next_send = time.time()
    accumulate_dur = 5.0  # TODO: make configurable?

    while not terminate_event.is_set():
        logging.debug("start of monitoring loop")
        try:
            d = accumulate_and_prepare()
            if time.time() >= next_send:
                logging.debug("Sending intermediate resource message")
                radio.send((MessageType.RESOURCE_INFO, d))
                next_send += sleep_dur
        except Exception:
            logging.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)
        logging.debug("sleeping")

        # wait either until approx next send time, or the accumulation period
        # so the accumulation period will not be completely precise.
        # but before this, the sleep period was also not completely precise.
        # with a minimum floor of 0 to not upset wait

        terminate_event.wait(max(0, min(next_send - time.time(), accumulate_dur)))

    logging.debug("Sending final resource message")
    try:
        d = accumulate_and_prepare()
        radio.send((MessageType.RESOURCE_INFO, d))
    except Exception:
        logging.exception("Exception getting the resource usage. Not sending final usage to Hub", exc_info=True)

    # TODO: write out any accumulated messages that might have been
    # accumulated by the results radio, so that the task wrapper in the main
    # task process can see these results.
    from parsl.monitoring.radios import result_radio_queue
    logging.debug("Sending result_radio_queue")
    terminate_queue.put(result_radio_queue)

    logging.debug("End of monitoring helper")
