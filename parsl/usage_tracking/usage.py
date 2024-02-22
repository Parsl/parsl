import uuid
import time
import os
import json
import logging
import socket
import sys
import platform

from parsl.utils import setproctitle
from parsl.multiprocessing import ForkProcess
from parsl.dataflow.states import States
from parsl.version import VERSION as PARSL_VERSION

logger = logging.getLogger(__name__)

from typing import Callable
from typing_extensions import ParamSpec

P = ParamSpec("P")


def async_process(fn: Callable[P, None]) -> Callable[P, None]:
    """ Decorator function to launch a function as a separate process """

    def run(*args, **kwargs):
        proc = ForkProcess(target=fn, args=args, kwargs=kwargs, name="Usage-Tracking")
        proc.start()
        return proc

    return run


@async_process
def udp_messenger(domain_name: str, UDP_PORT: int, sock_timeout: int, message: str) -> None:
    """Send UDP messages to usage tracker asynchronously

    This multiprocessing based messenger was written to overcome the limitations
    of signalling/terminating a thread that is blocked on a system call.

    Args:
          - domain_name (str) : Domain name string
          - UDP_PORT (int) : UDP port to send out on
          - sock_timeout (int) : Socket timeout
    """
    setproctitle("parsl: Usage tracking")

    try:
        encoded_message = bytes(message, "utf-8")

        UDP_IP = socket.gethostbyname(domain_name)

        if UDP_PORT is None:
            raise Exception("UDP_PORT is None")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.settimeout(sock_timeout)
        sock.sendto(encoded_message, (UDP_IP, UDP_PORT))
        sock.close()

    except socket.timeout:
        logger.debug("Failed to send usage tracking data: socket timeout")
    except OSError as e:
        logger.debug("Failed to send usage tracking data: OSError: {}".format(e))
    except Exception as e:
        logger.debug("Failed to send usage tracking data: Exception: {}".format(e))


class UsageTracker:
    """Usage Tracking for Parsl.

    The server for this is here: https://github.com/Parsl/parsl_tracking
    This issue captures the discussion that went into functionality
    implemented here: https://github.com/Parsl/parsl/issues/34

    """

    def __init__(self, dfk, port=50077,
                 domain_name='tracking.parsl-project.org'):
        """Initialize usage tracking unless the user has opted-out.

        We will try to resolve the hostname specified in kwarg:domain_name
        and if that fails attempt to use the kwarg:ip. Determining the
        IP and sending message happens in an asynchronous processs to avoid
        slowing down DFK initialization.

        Tracks usage stats by inspecting the internal state of the dfk.

        Args:
             - dfk (DFK object) : Data Flow Kernel object

        KWargs:
             - port (int) : Port number, Default:50077
             - domain_name (string) : Domain name, will override IP
                  Default: tracking.parsl-project.org
        """

        self.domain_name = domain_name
        # The sock timeout will only apply to UDP send and not domain resolution
        self.sock_timeout = 5
        self.UDP_PORT = port
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
        """Check if tracking is enabled.

        Tracking will be enabled unless either of these is true:

            1. dfk.config.usage_tracking is set to False
            2. Environment variable PARSL_TRACKING is set to false (case insensitive)

        """
        track = True

        if not self.config.usage_tracking:
            track = False

        envvar = str(os.environ.get("PARSL_TRACKING", True)).lower()
        if envvar == "false":
            track = False

        return track

    def construct_start_message(self) -> str:
        """Collect preliminary run info at the start of the DFK.

        Returns :
              - Message dict dumped as json string, ready for UDP
        """
        message = {'uuid': self.uuid,
                   'parsl_v': self.parsl_version,
                   'python_v': self.python_version,
                   'os': platform.system(),
                   'os_v': platform.release(),
                   'start': time.time()}

        return json.dumps(message)

    def construct_end_message(self) -> str:
        """Collect the final run information at the time of DFK cleanup.

        Returns:
             - Message dict dumped as json string, ready for UDP
        """
        app_count = self.dfk.task_count

        site_count = len(self.dfk.config.executors)

        app_fails = self.dfk.task_state_counts[States.failed] + self.dfk.task_state_counts[States.dep_fail]

        message = {'uuid': self.uuid,
                   'end': time.time(),
                   't_apps': app_count,
                   'sites': site_count,
                   'failed': app_fails
                   }

        return json.dumps(message)

    def send_UDP_message(self, message: str) -> None:
        """Send UDP message."""
        if self.tracking_enabled:
            try:
                proc = udp_messenger(self.domain_name, self.UDP_PORT, self.sock_timeout, message)
                self.procs.append(proc)
            except Exception as e:
                logger.debug("Usage tracking failed: {}".format(e))

    def send_message(self) -> None:
        """Send message over UDP.
        """
        if not self.initialized:
            message = self.construct_start_message()
            self.initialized = True
        else:
            message = self.construct_end_message()

        self.send_UDP_message(message)

    def close(self, timeout: float = 10.0) -> None:
        """First give each process one timeout period to finish what it is
        doing, then kill it (SIGKILL). There's no softer SIGTERM step,
        because that adds one join period of delay for what is almost
        definitely either: going to behave broadly the same as to SIGKILL,
        or won't respond to SIGTERM.
        """
        for proc in self.procs:
            proc.join(timeout=timeout)
            if proc.is_alive():
                logger.info("Usage tracking process did not end itself; sending SIGKILL")
                proc.kill()
