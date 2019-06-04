import os
import socket
import pickle
import logging
import time
import datetime
import zmq

import queue
from multiprocessing import Process, Queue
from parsl.utils import RepresentationMixin

from parsl.monitoring.message_type import MessageType

from typing import Optional

try:
    from parsl.monitoring.db_manager import dbm_starter
except Exception as e:
    _db_manager_excepts = e  # type: Optional[Exception]
else:
    _db_manager_excepts = None


def start_file_logger(filename, name='monitoring', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name. Default="parsl.executors.interchange"
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.

    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class UDPRadio(object):

    def __init__(self, monitoring_url, source_id=None, timeout=10):
        """
        Parameters
        ----------

        monitoring_url : str
            URL of the form <scheme>://<IP>:<PORT>
        message : py obj
            Python object to send, this will be pickled
        source_id : str
            String identifier of the source
        timeout : int
            timeout, default=10s
        """

        self.monitoring_url = monitoring_url
        self.sock_timeout = timeout
        self.source_id = source_id
        try:
            self.scheme, self.ip, port = (x.strip('/') for x in monitoring_url.split(':'))
            self.port = int(port)
        except Exception:
            raise Exception("Failed to parse monitoring url: {}".format(monitoring_url))

        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)  # UDP
        self.sock.settimeout(self.sock_timeout)

    def send(self, message_type, task_id, message):
        """ Sends a message to the UDP receiver

        Parameter
        ---------

        message_type: monitoring.MessageType (enum)
            In this case message type is RESOURCE_INFO most often
        task_id: int
            Task identifier of the task for which resource monitoring is being reported
        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            # bytes sent
        """
        x = 0
        try:
            buffer = pickle.dumps((self.source_id,   # Identifier for manager
                                   int(time.time()),  # epoch timestamp
                                   message_type,
                                   message))
        except Exception as e:
            print("Exception during pickling {}".format(e))
            return

        try:
            x = self.sock.sendto(buffer, (self.ip, self.port))
        except socket.timeout:
            print("Could not send message within timeout limit")
            return False
        return x

    def __del__(self):
        self.sock.close()


class MonitoringHub(RepresentationMixin):
    def __init__(self,
                 hub_address,
                 hub_port=None,
                 hub_port_range=(55050, 56000),

                 client_address="127.0.0.1",
                 client_port_range=(55000, 56000),

                 workflow_name=None,
                 workflow_version=None,
                 logging_endpoint='sqlite:///monitoring.db',
                 logdir=None,
                 logging_level=logging.INFO,
                 resource_monitoring_enabled=True,
                 resource_monitoring_interval=30):  # in seconds
        """
        Update docs here.
        """
        self.logger = None
        self._dfk_channel = None

        if _db_manager_excepts:
            raise(_db_manager_excepts)

        self.client_address = client_address
        self.client_port_range = client_port_range

        self.hub_address = hub_address
        self.hub_port = hub_port
        self.hub_port_range = hub_port_range

        self.logging_endpoint = logging_endpoint
        self.logdir = logdir
        self.logging_level = logging_level

        self.workflow_name = workflow_name
        self.workflow_version = workflow_version

        self.resource_monitoring_enabled = resource_monitoring_enabled
        self.resource_monitoring_interval = resource_monitoring_interval

    def start(self):

        if self.logdir is None:
            self.logdir = "."

        try:
            os.makedirs(self.logdir)
        except FileExistsError:
            pass

        # Initialize the ZMQ pipe to the Parsl Client
        self.logger = start_file_logger("{}/monitoring_hub.log".format(self.logdir),
                                        name="monitoring_hub",
                                        level=self.logging_level)
        self.logger.info("Monitoring Hub initialized")

        self.logger.debug("Initializing ZMQ Pipes to client")
        self.monitoring_hub_active = True
        self._context = zmq.Context()
        self._dfk_channel = self._context.socket(zmq.DEALER)
        self._dfk_channel.set_hwm(0)
        self.dfk_port = self._dfk_channel.bind_to_random_port("tcp://{}".format(self.client_address),
                                                              min_port=self.client_port_range[0],
                                                              max_port=self.client_port_range[1])

        comm_q = Queue(maxsize=10)
        self.stop_q = Queue(maxsize=10)
        self.priority_msgs = Queue()
        self.resource_msgs = Queue()

        self.queue_proc = Process(target=hub_starter,
                                  args=(comm_q, self.priority_msgs, self.resource_msgs, self.stop_q),
                                  kwargs={"hub_address": self.hub_address,
                                          "hub_port": self.hub_port,
                                          "hub_port_range": self.hub_port_range,
                                          "client_address": self.client_address,
                                          "client_port": self.dfk_port,
                                          "logdir": self.logdir,
                                          "logging_level": self.logging_level,
                                  },
        )
        self.queue_proc.start()

        self.dbm_proc = Process(target=dbm_starter,
                                args=(self.priority_msgs, self.resource_msgs,),
                                kwargs={"logdir": self.logdir,
                                        "logging_level": self.logging_level,
                                        "db_url": self.logging_endpoint,
                                  },
        )
        self.dbm_proc.start()

        try:
            udp_dish_port = comm_q.get(block=True, timeout=120)
        except queue.Empty:
            self.logger.error("Hub has not completed initialization in 120s. Aborting")
            raise Exception("Hub failed to start")

        self.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_dish_port)
        return self.monitoring_hub_url

    def send(self, mtype, message):
        self.logger.debug("Sending message {}, {}".format(mtype, message))
        return self._dfk_channel.send_pyobj((mtype, message))

    def close(self):
        if self.logger:
            self.logger.info("Terminating Monitoring Hub")
        if self._dfk_channel and self.monitoring_hub_active:
            self.monitoring_hub_active = False
            self._dfk_channel.close()
            self.logger.info("Waiting Hub to receive all messages and terminate")
            try:
                msg = self.stop_q.get()
                self.logger.info("Received {} from Hub".format(msg))
            except queue.Empty:
                pass
            self.logger.info("Terminating Hub")
            self.queue_proc.terminate()
            self.priority_msgs.put(("STOP", 0))

    def __del__(self):
        self.close()

    @staticmethod
    def monitor_wrapper(f, task_id, monitoring_hub_url, run_id, sleep_dur):
        """ Internal
        Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
        """
        def wrapped(*args, **kwargs):
            p = Process(target=monitor, args=(os.getpid(), task_id, monitoring_hub_url, run_id, sleep_dur))
            p.start()
            try:
                return f(*args, **kwargs)
            finally:
                # There's a chance of zombification if the workers are killed by some signals
                p.terminate()
                p.join()
        return wrapped


class Hub(object):

    def __init__(self,
                 hub_address,
                 hub_port=None,
                 hub_port_range=(55050, 56000),

                 database=None,              # Zhuozhao, can you put in the right default here?
                 visualization_server=None,  # Zhuozhao, can you put in the right default here?

                 client_address="127.0.0.1",
                 client_port=None,

                 monitoring_hub_address="127.0.0.1",
                 logdir=".",
                 logging_level=logging.DEBUG,
                 atexit_timeout=3    # in seconds
                ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        address : str
            IP address of the node on which the monitoring hub will run, this address must be
            reachable from the Parsl client as well as the worker nodes. Eg. <NNN>.<NNN>.<NNN>.<NNN>

        port : int
            Used with Elasticsearch logging, the port of where to access Elasticsearch. Required when using logging_type = 'elasticsearch'.

        logging_endpoint : Endpoint object
            This is generally a database object to which logging data can be pushed to from the
            monitoring HUB.

        workflow_name : str, optional
            Name to record as the workflow base name, defaults to the name of the parsl script file if left as None.

        workflow_version : str, optional
            Optional workflow identification to distinguish between workflows with the same name, not used internally only for display to user.

        atexit_timeout : float, optional
            The amount of time in seconds to terminate the hub without receiving any messages, after the last dfk workflow message is received.

        """
        try:
            os.makedirs(logdir)
        except FileExistsError:
            pass
        self.logger = start_file_logger("{}/hub.log".format(logdir),
                                        name="hub",
                                        level=logging_level)
        self.logger.debug("Hub starting")

        if not hub_port:
            self.logger.critical("At this point the hub port must be set")

        self.hub_port = hub_port
        self.hub_address = hub_address
        self.database = database
        self.visualization_server = visualization_server
        self.atexit_timeout = atexit_timeout

        self.loop_freq = 10.0  # milliseconds

        # Initialize the UDP socket
        self.logger.debug("Intiializing the UDP socket on 0.0.0.0:{}".format(hub_port))
        try:
            self.sock = socket.socket(socket.AF_INET,
                                      socket.SOCK_DGRAM,
                                      socket.IPPROTO_UDP)

            # We are trying to bind to all interfaces with 0.0.0.0
            self.sock.bind(('0.0.0.0', hub_port))
            self.sock.settimeout(self.loop_freq / 1000)
        except OSError:
            self.logger.critical("The port is already in use")
            self.hub_port = -1

        self._context = zmq.Context()
        self.dfk_channel = self._context.socket(zmq.DEALER)
        self.dfk_channel.set_hwm(0)
        self.dfk_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.dfk_channel.connect("tcp://{}:{}".format(client_address, client_port))

    def start(self, priority_msgs, resource_msgs, stop_q):

        while True:
            try:
                data, addr = self.sock.recvfrom(2048)
                msg = pickle.loads(data)
                resource_msgs.put((msg, addr))
                self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
            except socket.timeout:
                pass

            try:
                msg = self.dfk_channel.recv_pyobj()
                self.logger.debug("Got ZMQ Message: {}".format(msg))
                priority_msgs.put((msg, 0))
                if msg[0].value == MessageType.WORKFLOW_INFO.value and 'python_version' not in msg[1]:
                    break
            except zmq.Again:
                pass

        last_msg_received_time = time.time()
        while time.time() - last_msg_received_time < self.atexit_timeout:
            try:
                data, addr = self.sock.recvfrom(2048)
                msg = pickle.loads(data)
                resource_msgs.put((msg, addr))
                last_msg_received_time = time.time()
                self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
            except socket.timeout:
                pass
        stop_q.put("STOP")


def hub_starter(comm_q, priority_msgs, resource_msgs, stop_q, *args, **kwargs):
    hub = Hub(*args, **kwargs)
    comm_q.put(hub.hub_port)
    hub.start(priority_msgs, resource_msgs, stop_q)


def monitor(pid, task_id, monitoring_hub_url, run_id, sleep_dur=10):
    """Internal
    Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.
    """
    import psutil
    import platform

    radio = UDPRadio(monitoring_hub_url,
                     source_id=task_id)

    # these values are simple to log. Other information is available in special formats such as memory below.
    simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    # values that can be summed up to see total resources used by task process and its children
    summable_values = ['cpu_percent', 'memory_percent', 'num_threads']

    pm = psutil.Process(pid)
    pm.cpu_percent()

    first_msg = True

    while True:
        try:
            d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
            d["run_id"] = run_id
            d["task_id"] = task_id
            d['resource_monitoring_interval'] = sleep_dur
            d['hostname'] = platform.node()
            d['first_msg'] = first_msg
            d['timestamp'] = datetime.datetime.now()
            children = pm.children(recursive=True)
            d["psutil_cpu_count"] = psutil.cpu_count()
            d['psutil_process_memory_virtual'] = pm.memory_info().vms
            d['psutil_process_memory_resident'] = pm.memory_info().rss
            d['psutil_process_time_user'] = pm.cpu_times().user
            d['psutil_process_time_system'] = pm.cpu_times().system
            d['psutil_process_children_count'] = len(children)
            try:
                d['psutil_process_disk_write'] = pm.io_counters().write_bytes
                d['psutil_process_disk_read'] = pm.io_counters().read_bytes
            except psutil._exceptions.AccessDenied:
                # occassionally pid temp files that hold this information are unvailable to be read so set to zero
                d['psutil_process_disk_write'] = 0
                d['psutil_process_disk_read'] = 0
            for child in children:
                for k, v in child.as_dict(attrs=summable_values).items():
                    d['psutil_process_' + str(k)] += v
                d['psutil_process_time_user'] += child.cpu_times().user
                d['psutil_process_time_system'] += child.cpu_times().system
                d['psutil_process_memory_virtual'] += child.memory_info().vms
                d['psutil_process_memory_resident'] += child.memory_info().rss
                try:
                    d['psutil_process_disk_write'] += child.io_counters().write_bytes
                    d['psutil_process_disk_read'] += child.io_counters().read_bytes
                except psutil._exceptions.AccessDenied:
                    # occassionally pid temp files that hold this information are unvailable to be read so add zero
                    d['psutil_process_disk_write'] += 0
                    d['psutil_process_disk_read'] += 0

        finally:
            radio.send(MessageType.TASK_INFO, task_id, d)
            time.sleep(sleep_dur)
            first_msg = False
