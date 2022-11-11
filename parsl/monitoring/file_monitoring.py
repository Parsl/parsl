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
