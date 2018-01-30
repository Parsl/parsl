import uuid
import time
import hashlib
import os
import getpass
import json
import logging
import socket

from parsl.dataflow.states import States

logger = logging.getLogger(__name__)


class UsageTracker (object):
    """ Anonymized Usage Tracking for Parsl

    Client for this is here : https://github.com/Parsl/parsl_tracking
    This issue captures the discussion that went into functionality
    implemented here : https://github.com/Parsl/parsl/issues/34

    """

    def __init__(self, dfk, ip='52.3.111.203', port=50077,
                 domain_name='tracking.parsl-project.org'):
        ''' Initialize usage tracking unless the user has opted-out.
        Tracks usage stats by inspecting the internal state of the dfk.

        Args:
             - dfk (DFK object) : Data Flow Kernel object

        KWargs:
             - ip (string) : IP address
             - port (int) : Port number, Default:50077
             - domain_name (string) : Domain name, will override IP
                  Default: tracking.parsl-project.org
        '''
        if domain_name:
            try:
                self.UDP_IP = socket.gethostbyname(domain_name)
            except Exception:
                logging.debug("Could not lookup domain_name, defaulting to 52.3.111.203")
                self.UDP_IP = ip
        else:
            self.UDP_IP = ip
        self.UDP_PORT = port
        self.dfk = dfk
        self.config = self.dfk.config
        self.uuid = str(uuid.uuid4())
        self.test_mode, self.tracking_enabled = self.check_tracking_enabled()
        logger.debug("Tracking status: {}".format(self.tracking_enabled))
        logger.debug("Testing mode   : {}".format(self.test_mode))
        self.initialized = False  # Once first message is sent this will be True

    def check_tracking_enabled(self):
        ''' By default tracking is enabled.

        If Test mode is set via env variable PARSL_TESTING, a test flag is set

        Tracking is disabled if :
            1. config["globals"]["usageTracking"] is set to False (Bool)
            2. Environment variable PARSL_TRACKING is set to false (case insensitive)

        '''

        track = True   # By default we track usage
        test = False  # By default we are not in testing mode

        testvar = str(os.environ.get("PARSL_TESTING", 'None')).lower()
        if testvar == 'true':
            test = True

        if self.config and self.config["globals"]["usageTracking"] is False:
            track = False

        envvar = str(os.environ.get("PARSL_TRACKING", True)).lower()
        if envvar == "false":
            track = False

        return test, track

    def construct_start_message(self):
        """ Collect preliminary run info at the start of the DFK.

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
                   'test': self.test_mode,
                   'start': time.time()}

        return json.dumps(message)

    def construct_end_message(self):
        """ Collect the final run information at the time of DFK
        cleanup.

        Returns:
             - Message dict dumped as json string, ready for UDP
        """

        app_count = self.dfk.task_count

        site_count = 0
        if self.dfk._executors_managed:
            site_count = len(self.dfk.config['sites'])

        failed_states = (States.failed, States.dep_fail)
        app_fails = len([t for t in self.dfk.tasks if
                         self.dfk.tasks[t]['status'] in failed_states])

        message = {'uuid': self.uuid,
                   'end': time.time(),
                   't_apps': app_count,
                   'sites': site_count,
                   'c_time': None,
                   'failed': app_fails,
                   'test': self.test_mode,
                   }

        return json.dumps(message)

    def send_UDP_message(self, message):
        """ Send UDP message
        """
        if self.tracking_enabled:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
                x = sock.sendto(bytes(message, "utf-8"), (self.UDP_IP, self.UDP_PORT))
                sock.close()
            except OSError:
                logger.debug("Unable to reach the network to send usage data")
                x = 0
        else:
            x = -1

        return x

    def send_message(self):
        """ Send message over UDP.
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

        x = self.send_UDP_message(message)
        end = time.time()

        return x, end - start


if __name__ == '__main__':

    from parsl import *

    workers = ThreadPoolExecutor(max_workers=4)
    dfk = DataFlowKernel(executors=[workers])

    # ut = UsageTracker(dfk, ip='52.3.111.203')
    ut = UsageTracker(dfk, domain_name='tracking.parsl-project.org')

    for i in range(0, 2):
        x = ut.send_message()
        print(x)
