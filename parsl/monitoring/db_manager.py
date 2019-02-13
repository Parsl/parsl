import logging
import threading
import queue
import os
from enum import Enum

try:
    import sqlalchemy as sa
    from sqlalchemy import Table, Column, Text
except ImportError:
    pass

WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization


class MessageType(Enum):

    # Reports any task related info such as launch, completion etc.
    TASK_INFO = 0

    # Reports of resource utilization on a per-task basis
    RESOURCE_INFO = 1

    # Top level workflow information
    WORKFLOW_INFO = 2


class Database(object):
    def __init__(self,
                 url='sqlite:///monitoring.db',
                 username=None,
                 password=None,
            ):
        self.eng = sa.create_engine(url)
        self.meta = sa.MetaData()

        self.create_workflow_table(self.meta)
        self.create_task_table(self.meta)
        self.create_status_table(self.meta)
        self.create_resource_table(self.meta)
        self.meta.create_all(self.eng)
        self.meta.reflect(bind=self.eng)

        self.con = self.eng.connect()

    def execute(self, query):
        self.con.execute(query)

    def update(self, table=None, columns=[], where_condition=['run_id'], msg=None):
        table = self.meta.tables[table]
        query = table.update()
        for w in where_condition:
            query = query.where(table.c[w] == msg[w])
        query = query.values(**{k: msg[k] for k in columns})
        self.execute(query)

    def insert(self, table=None, msg=None):
        table = self.meta.tables[table]
        query = table.insert().values(**{k: v for k, v in msg.items() if k in table.c})
        self.execute(query)

    def create_workflow_table(self, meta):
        return Table(
                WORKFLOW, meta,
                Column('run_id', Text, nullable=False, primary_key=True),
                Column('workflow_name', Text, nullable=True),
                Column('workflow_version', Text, nullable=True),
                Column('time_began', Text, nullable=False),
                Column('time_completed', Text),
                Column('host', Text, nullable=False),
                Column('user', Text, nullable=False),
                Column('rundir', Text, nullable=False),
                Column('tasks_failed_count', Text, nullable=False),
                Column('tasks_completed_count', Text, nullable=False),
        )

    # TODO: expand to full set of info
    def create_status_table(self, meta):
        return Table(
              STATUS, meta,
              Column('task_id', Text, sa.ForeignKey('task.task_id'), nullable=False),
              Column('task_status_name', Text, nullable=False),
              Column('timestamp', Text, nullable=False),
              Column('run_id', Text, sa.ForeignKey('workflow.run_id'), nullable=False),
        )

    def create_task_table(self, meta):
        return Table(
              TASK, meta,
              Column('task_id', Text, nullable=False),
              Column('run_id', Text, sa.ForeignKey('workflow.run_id'), nullable=False),
              Column('task_executor', Text, nullable=False),
              Column('task_func_name', Text, nullable=False),
              Column('task_time_submitted', Text, nullable=False),
              Column('task_time_returned', Text, nullable=True),
              Column('task_memoize', Text, nullable=False),
              Column('task_inputs', Text, nullable=True),
              Column('task_outputs', Text, nullable=True),
              Column('task_stdin', Text, nullable=True),
              Column('task_stdout', Text, nullable=True),
        )

    def create_resource_table(self, meta):
        return Table(
              RESOURCE, meta,
              Column('task_id', Text, sa.ForeignKey('task.task_id'), nullable=False),
              Column('timestamp', Text, nullable=False),
              Column('run_id', Text, sa.ForeignKey('workflow.run_id'), nullable=False),
              Column('psutil_process_pid', Text, nullable=True),
              Column('psutil_process_cpu_percent', Text, nullable=True),
              Column('psutil_process_memory_percent', Text, nullable=True),
              Column('psutil_process_children_count', Text, nullable=True),
              Column('psutil_process_time_user', Text, nullable=True),
              Column('psutil_process_time_system', Text, nullable=True),
              Column('psutil_process_memory_virtual', Text, nullable=True),
              Column('psutil_process_memory_resident', Text, nullable=True),
              Column('psutil_process_disk_read', Text, nullable=True),
              Column('psutil_process_disk_write', Text, nullable=True),
              Column('psutil_process_status', Text, nullable=True),
        )

    def __del__(self):
        self.con.close()


class DatabaseManager(object):
    def __init__(self,
                 db_url='sqlite:///monitoring.db',
                 logdir='.',
                 logging_level=logging.INFO,
               ):

        self.logdir = logdir
        try:
            os.makedirs(self.logdir)
        except FileExistsError:
            pass

        self.logger = start_file_logger("{}/database_manager.log".format(self.logdir), level=logging_level)
        self.logger.debug("Initializing Database Manager process")

        self.db = Database(db_url)

        self.pending_priority_queue = queue.Queue()
        self.pending_resource_queue = queue.Queue()

    def start(self, priority_queue, resource_queue):
        self._priority_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                            args=(priority_queue, 'priority',)
                                              )
        self._priority_queue_pull_thread.start()

        self._resource_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                            args=(resource_queue, 'resource',)
                                              )
        self._resource_queue_pull_thread.start()

        while True:
            try:
                x, addr = self.pending_priority_queue.get(block=False)
                self.logger.debug("Database manager receives a priority message {}".format(x))
            except queue.Empty:
                pass
            else:
                msg_type, msg = x[0], x[1]
                self.logger.debug("The message type is {}".format(msg_type))
                if msg_type.value == MessageType.WORKFLOW_INFO.value:
                    if "python_version" in msg:   # workflow start
                        self.logger.debug("Inserting workflow start info to WORKFLOW table")
                        self._insert(table=WORKFLOW, msg=msg)

                    else:                         # workflow end
                        self.logger.debug("Updating workflow end info to WORKFLOW table")
                        self._update(table=WORKFLOW,
                                     columns=['tasks_failed_count', 'tasks_completed_count', 'time_completed'],
                                     where_condition=['run_id'],
                                     msg=msg)
                elif msg_type.value == MessageType.TASK_INFO.value:
                    self.logger.debug("Updating and inserting TASK_INFO to all tables")
                    self._update(table=WORKFLOW,
                                 columns=['tasks_failed_count', 'tasks_completed_count'],
                                 where_condition=['run_id'],
                                 msg=msg)
                    if msg['task_time_returned'] is not None:
                        self._update(table=TASK,
                                     columns=['task_time_returned'],
                                     where_condition=['run_id', 'task_id'],
                                     msg=msg)
                    else:
                        self._insert(TASK, msg)
                    self._insert(STATUS, msg)
            try:
                x, _ = self.pending_resource_queue.get(block=False)
                self.logger.debug("Database manager receives a resource message {}".format(x))
            except queue.Empty:
                pass
            else:
                msg = x[-1]
                self._insert(RESOURCE, msg)
                # self._insert(STATUS, msg)

    def _migrate_logs_to_internal(self, logs_queue, queue_tag):
        self.logger.info("[{}_queue_PULL_THREAD] Starting".format(queue_tag))

        while True:
            try:
                x = logs_queue.get(block=False)
            except queue.Empty:
                continue
            else:
                if queue_tag == 'priority':
                    self.pending_priority_queue.put(x)
                elif queue_tag == 'resource':
                    self.pending_resource_queue.put(x)

    def _update(self, table, columns, where_condition, msg):
        self.db.update(table=table, columns=columns, where_condition=where_condition, msg=msg)

    def _insert(self, table, msg):
        self.db.insert(table=table, msg=msg)

    def __del__(self):
        if self.logger:
            self.logger.info("Terminating Database Manager")


def start_file_logger(filename, name='database_manager', level=logging.DEBUG, format_string=None):
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
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def dbm_starter(priority_msgs, resource_msgs, *args, **kwargs):
    """Start the database manager process

    The DFK should start this function. The args, kwargs match that of the monitoring config

    """
    dbm = DatabaseManager(*args, **kwargs)
    dbm.start(priority_msgs, resource_msgs)
