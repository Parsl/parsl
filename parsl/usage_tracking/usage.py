import json
import logging
import platform
import socket
import sys
import time
import uuid

from parsl.dataflow.states import States
from parsl.errors import ConfigurationError
from parsl.multiprocessing import ForkProcess
from parsl.usage_tracking.api import get_parsl_usage
from parsl.usage_tracking.levels import DISABLED as USAGE_TRACKING_DISABLED
from parsl.usage_tracking.levels import LEVEL_3 as USAGE_TRACKING_LEVEL_3
from parsl.utils import setproctitle
from parsl.version import VERSION as PARSL_VERSION

logger = logging.getLogger(__name__)

from typing import Callable

from typing_extensions import ParamSpec

# protocol version byte: when (for example) compression parameters are changed
# that cannot be inferred from the compressed message itself, this version
# ID needs to imply those parameters.

# Earlier protocol versions: b'{' - the original pure-JSON protocol pre-March 2024
PROTOCOL_VERSION = b'1'

P = ParamSpec("P")


def async_process(fn: Callable[P, None]) -> Callable[P, None]:
    """ Decorator function to launch a function as a separate process """

    def run(*args, **kwargs):
        proc = ForkProcess(target=fn, args=args, kwargs=kwargs, name="Usage-Tracking")
        proc.start()
        return proc

    return run


@async_process
def udp_messenger(domain_name: str, UDP_PORT: int, sock_timeout: int, message: bytes) -> None:
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
        UDP_IP = socket.gethostbyname(domain_name)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.settimeout(sock_timeout)
        sock.sendto(message, (UDP_IP, UDP_PORT))
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
        self.correlator_uuid = str(uuid.uuid4())
        self.parsl_version = PARSL_VERSION
        self.python_version = "{}.{}.{}".format(sys.version_info.major,
                                                sys.version_info.minor,
                                                sys.version_info.micro)
        self.tracking_level = self.check_tracking_level()
        self.project_name = self.config.project_name
        self.start_time = None
        logger.debug("Tracking level: {}".format(self.tracking_level))

    def check_tracking_level(self) -> int:
        """Check if tracking is enabled and return level.

        Checks usage_tracking in Config
            - Possible values: [True, False, 0, 1, 2, 3]

        True/False values are treated as Level 1/Level 0 respectively.

        Returns: int
            - 0 : Tracking is disabled
            - 1 : Tracking is enabled with level 1
                  Share info about Parsl version, Python version, platform
            - 2 : Tracking is enabled with level 2
                  Share info about config + level 1
            - 3 : Tracking is enabled with level 3
                  Share info about app count, app fails, execution time + level 2
        """
        if not USAGE_TRACKING_DISABLED <= self.config.usage_tracking <= USAGE_TRACKING_LEVEL_3:
            raise ConfigurationError(
                f"Usage Tracking values must be 0, 1, 2, or 3 and not {self.config.usage_tracking}"
            )

        return self.config.usage_tracking

    def construct_start_message(self) -> bytes:
        """Collect preliminary run info at the start of the DFK.

        Returns :
              - Message dict dumped as json string, ready for UDP
        """
        message = {'correlator': self.correlator_uuid,
                   'parsl_v': self.parsl_version,
                   'python_v': self.python_version,
                   'platform.system': platform.system(),
                   'tracking_level': int(self.tracking_level)}

        if self.project_name:
            message['project_name'] = self.project_name

        if self.tracking_level >= 2:
            message['components'] = get_parsl_usage(self.dfk._config)

        if self.tracking_level == 3:
            self.start_time = int(time.time())
            message['start'] = self.start_time

        logger.debug(f"Usage tracking start message: {message}")

        return self.encode_message(message)

    def construct_end_message(self) -> bytes:
        """Collect the final run information at the time of DFK cleanup.
        This is only called if tracking level is 3.

        Returns:
             - Message dict dumped as json string, ready for UDP
        """
        end_time = int(time.time())

        app_count = self.dfk.task_count

        app_fails = self.dfk.task_state_counts[States.failed] + self.dfk.task_state_counts[States.dep_fail]

        # the DFK is tangled into this code as a god-object, so it is
        # handled separately from the usual traversal code, but presenting
        # the same protocol-level report.
        dfk_component = {'c': type(self.dfk).__module__ + "." + type(self.dfk).__name__,
                         'app_count': app_count,
                         'app_fails': app_fails}

        message = {'correlator': self.correlator_uuid,
                   'end': end_time,
                   'execution_time': end_time - self.start_time,
                   'components': [dfk_component] + get_parsl_usage(self.dfk._config)}

        if self.project_name:
            message['project_name'] = self.project_name

        logger.debug(f"Usage tracking end message (unencoded): {message}")

        return self.encode_message(message)

    def encode_message(self, obj):
        return PROTOCOL_VERSION + json.dumps(obj).encode()

    def send_UDP_message(self, message: bytes) -> None:
        """Send UDP message."""
        try:
            proc = udp_messenger(self.domain_name, self.UDP_PORT, self.sock_timeout, message)
            self.procs.append(proc)
        except Exception as e:
            logger.debug("Usage tracking failed: {}".format(e))

    def send_start_message(self) -> None:
        if self.tracking_level:
            logger.info("Sending start message for usage tracking")
            self.start_time = time.time()
            message = self.construct_start_message()
            self.send_UDP_message(message)

    def send_end_message(self) -> None:
        if self.tracking_level == 3:
            logger.info("Sending end message for usage tracking")
            message = self.construct_end_message()
            self.send_UDP_message(message)

    def close(self, timeout: float = 10.0) -> None:
        """First give each process one timeout period to finish what it is
        doing, then kill it (SIGKILL). There's no softer SIGTERM step,
        because that adds one join period of delay for what is almost
        definitely either: going to behave broadly the same as to SIGKILL,
        or won't respond to SIGTERM.
        """
        if self.tracking_level:
            logger.info("Closing usage tracking")

            for proc in self.procs:
                logger.debug("Joining usage tracking process %s", proc)
                proc.join(timeout=timeout)
                if proc.is_alive():
                    logger.warning("Usage tracking process did not end itself; sending SIGKILL")
                    proc.kill()

                proc.close()
