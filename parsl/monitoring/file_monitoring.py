import datetime
import logging
import glob
from functools import wraps
import typeguard
import socket
import os
import time
import multiprocessing as mp
from multiprocessing import pool
import dill
import re
from typing import List, Callable, Any, Dict, Union, Optional
from parsl.multiprocessing import ForkProcess
import smtplib
from email.message import EmailMessage


logger = logging.getLogger(__name__)
logger.setLevel(logging.NOTSET)


def validate_email(addr: str) -> bool:
    """Simple email syntactical validator

    Parameters
    ----------
    addr : str
        The string to test for a valid email address.

    Returns
    -------
    bool
        True if addr is a syntactically valid email address.
    """
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.fullmatch(regex, addr) is not None


class Emailer:
    """The Emailer is a static class used to hold information about emailing notifications.

    It holds data regarding the email address to send to and whether to use SSL or not.

    """
    _useSSL = False  # use SSL or not
    _task_id = None  # task id of the current Parsl task
    _email = None    # email address to use

    def __new__(cls):
        raise TypeError('Static classes cannot be instantiated')

    @staticmethod
    def create(addr: str, tid: int) -> None:
        """ Initialize the class with an email address and task_id

        Email validation is performed.

        Parameters
        ----------
        addr : str
            The recipient's email address
        tid : int
            The current Parsl task id

        """
        if not validate_email(addr):
            Emailer._email = None
            return
        Emailer._email = addr
        Emailer._task_id = tid

    @staticmethod
    def invalidate() -> None:
        """Force the email address to be invalid

        """
        Emailer._email = None

    @staticmethod
    def is_valid() -> bool:
        """Return whether the email address is valid or not.

        Returns
        -------
        bool
            True if the email address is currently valid (not None)
        """
        return Emailer._email is not None

    @staticmethod
    def set_ssl(value: bool = True) -> None:
        """Set the value of the SSL flag, indicating whether SSL should eb used or not.

        Paramters
        ---------
        value : bool
            The value to set the SSL flag to (True, default, indicates that SSL should be used)
        """
        Emailer._useSSL = value

    @staticmethod
    def get_ssl() -> bool:
        """Return whether the SSL flag is set

        Returns
        -------
        bool
            True if the SSL flag is set
        """
        return Emailer._useSSL

    @staticmethod
    def get_task_id() -> int:
        """Returns the current Parsl task id

        Returns
        -------
        int
            The current Parsl task id.
        """
        return Emailer._task_id


def proc_callback(res: Union[str, bytes, bytearray]) -> None:
    """Callback function for the results of running a function in the Pool.

       Sends an email giving the results of the monitoring run. If an error is thrown
       when trying to create a smtp connection then it tries a smtp_ssl connection. If
       this throws an error then no email is sent and future calls to this function will
       just log and return.

       Parameters
       ----------
       res: Union[str, bytes, bytearray]
           The message to send as the body of the email. Can really be anything that can be
           cast directly to a string.
    """
    # if there is no message
    if not res:
        return
    # type check the input and cast as appropriate
    if not isinstance(res, str):
        if isinstance(res, (bytes, bytearray)):
            try:
                res = res.decode()
            except Exception as ex:
                logger.error(f"Could not decode bytes like object: {str(ex)}")
                return
        else:
            try:
                res = str(res)
            except Exception as ex:
                logger.error(f"Could not turn data into string: {str(ex)}")
                return
    # if there is no valid email or it has been disabled just log and return
    if not Emailer.is_valid():
        logging.info(res)
        return

    # if a ssl connection is needed
    if Emailer.get_ssl():
        try:
            smtp = smtplib.SMTP_SSL('localhost')
        except Exception as ex:
            logger.warning(f"Could not establish an SMTP_SSL connection: {str(ex)}")
            Emailer.invalidate()
            return
    else:
        # try generic connection
        try:
            smtp = smtplib.SMTP('localhost')
        except Exception as _:
            # try an ssl connection
            try:
                smtp = smtplib.SMTP_SSL('localhost')
                Emailer.set_ssl(True)
            except Exception as ex:
                # cannot may a valid connection so disable future attempts
                logger.warning(f"Could not establish an SMTP_SSL connection: {str(ex)}")
                Emailer.invalidate()
                return
    # construct the message and send
    try:
        email = EmailMessage()
        email.set_content(res)
        email['Subject'] = f"Processed files from Parsl task {Emailer.get_task_id()} running on {socket.gethostname()}"
        email['From'] = email
        email['To'] = email
        smtp.send_message(email)
        smtp.quit()
    except Exception as ex:
        # issue with email, so just log it
        logger.warning(f"Could not send email: {str(ex)}")
        logger.info(res)


def run_dill_encoded(payload: str) -> Any:
    """Take the dill encoded payload, decode, and run it.

    Parameters
    ----------
    payload: str
        String generated by calling dill.dumps

    Returns
    -------
    Any
        The result of running the decoded function
    """
    fun, args = dill.loads(payload)
    return fun(*args)


def apply_async(monitor_pool: mp.Pool, fun: Callable, args: Any) -> pool.AsyncResult:
    """Encode the given function and run it asynchronously.

    This is used to get around the pickl issue of not being able to encode functions that
    are not visible at the global scope.

    Parameters
    ----------
    monitor_pool: mp.Pool
        Pool object use to run the function
    fun: Callable
        The function to encode and run
    args: Any
        The function arguments

    Returns
    -------
    mp.Pool.AsyncResult
    """
    payload = dill.dumps((fun, args))
    return monitor_pool.apply_async(run_dill_encoded, (payload,), callback=proc_callback)


@typeguard.typechecked
def monitor(task_id: int,
            stop_event: mp.Event,
            done_event: mp.Event,
            patterns: List[Any],
            callbacks: List[Callable],
            sleep_dur: float,
            work_dir: str = None,
            email: Optional[str] = None):
    Emailer.create(email, task_id)
    if work_dir is not None:
        os.chdir(work_dir)
    fh = open("/g/g20/friedel2/isolator/smoke_test/mon.log", 'w')
    fh.write("starting monitor\n")
    fh.flush()
    monitor_pool = mp.Pool(len(patterns))

    logger.info(f"Monitor host {socket.gethostname()} started for task {task_id}  {type(patterns)}  {len(patterns)}")
    fh.write(f"Monitor host {socket.gethostname()} started for task {task_id}  {type(patterns)}  {len(patterns)}\n")
    fh.flush()
    found = []
    keep_running = True
    logger.info(f"Monitor host running")
    running_procs = []  # keep track of all callback runs
    fh.write(f"DIR {os.getcwd()}\n")
    while keep_running:
        logger.info(f"Monitor host running 1")
        fh.write(f"Monitor host running 1\n")
        fh.flush()
        # see if the function has been told to stop
        keep_running = not stop_event.is_set()
        if not keep_running:
            logger.info(f"  {stop_event.is_set()} received {str(datetime.datetime.now())}  {len(patterns)}")
            fh.write(f"  {stop_event.is_set()} received {str(datetime.datetime.now())}  {len(patterns)}\n")
            fh.flush()
            sleep_dur = 0
        # loop over all the patterns looking for new matches
        for i in range(len(patterns)):
            logger.info(f" {i} {patterns[i]}")
            fh.write(f" {i} {patterns[i]}\n")
            fh.flush()
            xfer = []
            current_time = time.time()
            # look for any matching files
            if patterns[i][1]:
                fh.write("REGEX\n")
                temp = [f for f in os.listdir(os.getcwd()) if patterns[i][0].search(f)]
            else:
                fh.write("NOT REGEX\n")
                temp = glob.glob(patterns[i][0])
            fh.flush()
            # weed out those that have been found before and any that are too new
            for t in temp:
                if t in found:
                    continue
                mtime = os.path.getmtime(t)
                # make sure the file is done.
                if mtime + sleep_dur < current_time:
                    xfer.append(t)
            if not xfer:
                logger.info(f"No files found for processing task {task_id}, pattern {i}.")
                fh.write(f"No files found for processing task {task_id}, pattern {i}.\n")
                fh.flush()
                continue
            # matches were found, sending them to callback
            fh.write(f"\n\nCalling async {xfer}\n\n")
            fh.flush()
            running_procs.append(apply_async(monitor_pool, callbacks[i], (xfer,)))
            found += xfer
        if not keep_running:
            # the loop is terminating, wait for callbacks to finish
            monitor_pool.close()
            monitor_pool.join()
            monitor_pool.terminate()
        else:
            time.sleep(sleep_dur)
    logger.info(f"HALT called for task {task_id}")
    fh.write(f"HALT called for task {task_id}\n")
    fh.close()
    # signal the function is complete
    done_event.set()


@typeguard.typechecked
class FileMonitor:
    def __init__(self,
                 callback: Union[Callable, List[Callable]],
                 pattern: Optional[Union[str, List[str]]] = None,
                 filetype: Optional[Union[str, List[str]]] = None,
                 path: Optional[str] = None,
                 working_dir: Optional[str] = None,
                 email: Optional[str] = None,
                 sleep_dur: float = 3.):
        logger.info(f"file_monitor initialized  {type(pattern)}  {type(filetype)}")
        self.email = email
        # generate the master pattern list
        self.patterns = []
        if pattern is not None:
            if isinstance(pattern, list):
                for p in pattern:
                    self.patterns.append((re.compile(p), True))
            else:
                self.patterns = [(re.compile(pattern), True)]
        if filetype is not None:
            temppat = []
            if isinstance(filetype, list):
                temppat = filetype
            else:
                temppat.append(filetype)
            if path is not None:
                for i, pat in enumerate(temppat):
                    if '*' not in pat:
                        if not pat.startswith('.'):
                            pat = '.' + pat
                        pat = '*' + pat
                    temppat[i] = os.path.join(path, pat)
            else:
                for i, pat in enumerate(temppat):
                    if '*' not in pat:
                        if not pat.startswith('.'):
                            pat = '.' + pat
                        pat = '*' + pat
                    temppat[i] = pat
            for ft in temppat:
                self.patterns.append((ft, False))
        if not self.patterns:
            raise Exception("Either pattern or filetype must be given")
        if isinstance(callback, list):
            if len(self.patterns) != len(callback):
                raise Exception("Pattern list is not the same size as function list")
            self.callbacks = callback
        else:
            self.callbacks = [callback] * len(self.patterns)
        self.sleep_dur = sleep_dur
        self.cwd = working_dir

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
                                   self.cwd,
                                   self.email))
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
