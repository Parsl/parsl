import datetime
import logging
import glob
from functools import wraps
import typeguard
import socket
import os
import time
import multiprocessing as mp
import re
from typing import List, Callable, Any, Dict, Union, Optional
from parsl.multiprocessing import ForkProcess

logger = logging.getLogger(__name__)
logger.setLevel(logging.NOTSET)


def proc_callback(res):
    if isinstance(res, list):
        for i in res:
            logger.info(i)
    elif isinstance(res, str):
        logger.info(res)
    elif isinstance(res, Exception):
        logger.error(str(res))


@typeguard.typechecked
def monitor(task_id: int,
            stop_event: mp.Event,
            done_event: mp.Event,
            patterns: List[Any],
            callbacks: List[Callable],
            sleep_dur: float,
            is_regex: bool):
    pool = mp.Pool(len(patterns))
    logger.info(f"Monitor host {socket.gethostname()} started for task {task_id}  {type(patterns)}  {len(patterns)}")
    found = []
    # cwd = os.getcwd()
    keep_running = True
    logger.info(f"Monitor host running")

    while keep_running:
        logger.info(f"Monitor host running 1")
        keep_running = not stop_event.is_set()

        logger.info(f"  {stop_event.is_set()} received {str(datetime.datetime.now())}  {len(patterns)}")
        time.sleep(sleep_dur)
        for i in range(len(patterns)):
            logger.info(f" {i} {patterns[i]}")
            xfer = []
            current_time = time.time()
            if is_regex:
                temp = [f for f in os.listdir(os.getcwd()) if patterns[i].search(f)]
            else:
                temp = glob.glob(patterns[i])
            for t in temp:
                if t in found:
                    continue
                mtime = os.path.getmtime(t)
                # make sure the file is done.
                if mtime + sleep_dur < current_time:
                    xfer.append(t)
            if not xfer:
                logger.info(f"No files found for processing task {task_id}, pattern {i}.")
                continue
            pool.apply_async(callbacks[i], (xfer,), callback=proc_callback, error_callback=proc_callback)
        time.sleep(sleep_dur)
    logger.info(f"HALT called for task {task_id}")
    done_event.set()
    time.sleep(3)


@typeguard.typechecked
class FileMonitor:
    def __init__(self,
                 callback: Union[Callable, List[Callable]],
                 pattern: Optional[Union[str, List[str]]] = None,
                 filetype: Optional[Union[str, List[str]]] = None,
                 path: Optional[str] = None,
                 sleep_dur: float = 3.):
        logger.info(f"file_monitor initialized  {type(pattern)}  {type(filetype)}")

        if pattern is not None:
            if filetype:
                raise Exception("Cannot specify both filetype and pattern")
            if isinstance(pattern, list):
                self.patterns = []
                for p in pattern:
                    self.patterns.append(re.compile(p))
                self.patterns = pattern
            else:
                self.patterns = [re.compile(pattern)]
            self.regex = True
        elif filetype:
            if pattern:
                raise Exception("")
            if isinstance(filetype, list):
                self.patterns = filetype
            else:
                self.patterns = [filetype]
            if path is not None:
                for i, pat in enumerate(self.patterns):
                    if '*' not in pat:
                        if not pat.startswith('.'):
                            pat = '.' + pat
                        pat = '*' + pat
                    self.patterns[i] = os.path.join(path, pat)
            else:
                for i, pat in enumerate(self.patterns):
                    if '*' not in pat:
                        if not pat.startswith('.'):
                            pat = '.' + pat
                        pat = '*' + pat
                    self.patterns[i] = pat
            self.regex = False
        else:
            raise Exception("Either pattern or filetype must be given")
        if isinstance(callback, list):
            if len(pattern) != len(callback):
                raise Exception("Pattern list is not the same size as function list")
            self.callbacks = callback
        else:
            self.callbacks = [callback] * len(self.patterns)
        self.sleep_dur = sleep_dur

    def file_monitor(self,
                     f: Any,
                     task_id: int) -> Callable:
        logger.info(f"file_monitor called  {len(self.patterns)}")

        @wraps(f)
        def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            ev = mp.Event()
            done = mp.Event()
            pp = ForkProcess(target=monitor,
                             args=(task_id,
                                   ev,
                                   done,
                                   self.patterns,
                                   self.callbacks,
                                   self.sleep_dur,
                                   self.regex))
            pp.start()
            try:
                return f(*args, **kwargs)
            finally:
                ev.set()
                pp.join(30)
                if pp.exitcode is None:
                    pp.terminate()
                    pp.join()
        return wrapped
