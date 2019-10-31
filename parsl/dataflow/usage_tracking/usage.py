import uuid
import time
import hashlib
import os
import getpass
import json
import logging
import socket
import sys
import platform
import multiprocessing as mp

from parsl.dataflow.states import FINAL_FAILURE_STATES
from parsl.version import VERSION as PARSL_VERSION

logger = logging.getLogger(__name__)


def async_process(fn):
    """ Decorator function to launch a function as a separate process """

    def run(*args, **kwargs):
        proc = mp.Process(target=fn, args=args, kwargs=kwargs, name="Usage-Tracking")
        proc.start()
        return proc

    return run


@async_process
def udp_messenger(domain_name, UDP_IP, UDP_PORT, sock_timeout, message):
    """Send UDP messages to usage tracker asynchronously

    This multiprocessing based messenger was written to overcome the limitations
    of signalling/terminating a thread that is blocked on a system call. This
    messenger is created as a separate process, and initialized with 2 queues,
    to_send to receive messages to be sent to the internet.

    Args:
          - domain_name (str) : Domain name string
          - UDP_IP (str) : IP address YYY.YYY.YYY.YYY
          - UDP_PORT (int) : UDP port to send out on
          - sock_timeout (int) : Socket timeout
          - to_send (multiprocessing.Queue) : Queue of outgoing messages to internet
    """
    try:
        if message is None:
            raise ValueError("message was none")

        encoded_message = bytes(message, "utf-8")

        if encoded_message is None:
            raise ValueError("utf-8 encoding of message failed")

        if domain_name:
            try:
                UDP_IP = socket.gethostbyname(domain_name)
            except Exception:
                # (False, "Domain lookup failed, defaulting to {0}".format(UDP_IP))
                pass

        if UDP_IP is None:
            raise Exception("UDP_IP is None")

        if UDP_PORT is None:
            raise Exception("UDP_PORT is None")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.settimeout(sock_timeout)
        sock.sendto(bytes(message, "utf-8"), (UDP_IP, UDP_PORT))
        sock.close()

    except socket.timeout:
        logger.debug("Failed to send usage tracking data: socket timeout")
    except OSError as e:
        logger.debug("Failed to send usage tracking data: OSError: {}".format(e))
    except Exception as e:
        logger.debug("Failed to send usage tracking data: Exception: {}".format(e))


class UsageTracker (object):
    """Anonymized Usage Tracking for Parsl.

    Client for this is here : https://github.com/Parsl/parsl_tracking
    This issue captures the discussion that went into functionality
    implemented here : https://github.com/Parsl/parsl/issues/34

    """

    def __init__(self, dfk, ip='52.3.111.203', port=50077,
                 domain_name='tracking.parsl-project.org'):
        """Initialize usage tracking unless the user has opted-out.

        We will try to resolve the hostname specified in kwarg:domain_name
        and if that fails attempt to use the kwarg:ip. Determining the
        IP and sending message is threaded to avoid slowing down DFK
        initialization.

        Tracks usage stats by inspecting the internal state of the dfk.

        Args:
             - dfk (DFK object) : Data Flow Kernel object

        KWargs:
             - ip (string) : IP address
             - port (int) : Port number, Default:50077
             - domain_name (string) : Domain name, will override IP
                  Default: tracking.parsl-project.org
        """

        self.domain_name = domain_name
        self.ip = ip
        # The sock timeout will only apply to UDP send and not domain resolution
        self.sock_timeout = 5
        self.UDP_PORT = port
        self.UDP_IP = None
        self.procs = []
        self.dfk = dfk
        self.config = self.dfk.config
        self.uuid = str(uuid.uuid4())
        self.parsl_version = PARSL_VERSION
        self.python_version = "{}.{}.{}".format(sys.version_info.major,
                                                sys.version_info.minor,
                                                sys.version_info.micro)
        self.tracking_enabled = self.check_tracking_enabled()
        logger.debug("Tracking status: {}".format(self.tracking_enabled))
        self.initialized = False  # Once first message is sent this will be True

    def check_tracking_enabled(self):
        """By default tracking is enabled.

        Tracking is disabled if :
            1. config["globals"]["usageTracking"] is set to False (Bool)
            2. Environment variable PARSL_TRACKING is set to false (case insensitive)

        """
        track = True   # By default we track usage

        if not self.config.usage_tracking:
            track = False

        envvar = str(os.environ.get("PARSL_TRACKING", True)).lower()
        if envvar == "false":
            track = False

        return track

    def construct_start_message(self):
        """Collect preliminary run info at the start of the DFK.

        Returns :
              - Message dict dumped as json string, ready for UDP
        """
        uname = getpass.getuser().encode('latin1')
        hashed_username = hashlib.sha256(uname).hexdigest()[0:10]
        hname = socket.gethostname().encode('latin1')
        hashed_hostname = hashlib.sha256(hname).hexdigest()[0:10]
        message = {'uuid': self.uuid,
                   'uname': hashed_username,
                   'hname': hashed_hostname,
                   'test': False,  # this field previously indicated if parsl
                                   # was being run in test mode, and is
                                   # retained for protocol compatibility
                   'parsl_v': self.parsl_version,
                   'python_v': self.python_version,
                   'os': platform.system(),
                   'os_v': platform.release(),
                   'start': time.time()}

        return json.dumps(message)

    def construct_end_message(self):
        """Collect the final run information at the time of DFK cleanup.

        Returns:
             - Message dict dumped as json string, ready for UDP
        """
        app_count = self.dfk.task_count

        site_count = len([x for x in self.dfk.config.executors if x.managed])

        app_fails = len([t for t in self.dfk.tasks if
                         self.dfk.tasks[t]['status'] in FINAL_FAILURE_STATES])

        message = {'uuid': self.uuid,
                   'end': time.time(),
                   't_apps': app_count,
                   'sites': site_count,
                   'c_time': None,
                   'failed': app_fails,
                   'test': False,  # see comment in construct_start_message
                   }

        return json.dumps(message)

    def send_UDP_message(self, message):
        """Send UDP message."""
        x = 0
        if self.tracking_enabled:
            try:
                proc = udp_messenger(self.domain_name, self.UDP_IP, self.UDP_PORT, self.sock_timeout, message)
                self.procs.append(proc)
            except Exception as e:
                logger.debug("Usage tracking failed: {}".format(e))
        else:
            x = -1

        return x

    def send_message(self):
        """Send message over UDP.

        If tracking is disables, the bytes_sent will always be set to -1

        Returns:
            (bytes_sent, time_taken)
        """
        start = time.time()
        message = None
        if not self.initialized:
            message = self.construct_start_message()
            self.initialized = True
        else:
            message = self.construct_end_message()

        self.send_UDP_message(message)
        end = time.time()

        return end - start

    def __del__(self):
        return self.close()

    def close(self):
        """We terminate (SIGTERM) the processes added to the self.procs list """
        for proc in self.procs:
            proc.terminate()
