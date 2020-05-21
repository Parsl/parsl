import logging
import threading
import queue
import os
import time
import datetime

from parsl.log_utils import set_file_logger
from parsl.dataflow.states import States
from parsl.providers.error import OptionalModuleMissing
from parsl.monitoring.message_type import MessageType

logger = logging.getLogger("database_manager")

try:
    import sqlalchemy as sa
    from sqlalchemy import Column, Text, Float, Boolean, Integer, DateTime, PrimaryKeyConstraint
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    _sqlalchemy_enabled = False
else:
    _sqlalchemy_enabled = True

try:
    from sqlalchemy_utils import get_mapper
except ImportError:
    _sqlalchemy_utils_enabled = False
else:
    _sqlalchemy_utils_enabled = True

WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization
NODE = 'node'            # Node table include node info


class Database:

    if not _sqlalchemy_enabled:
        raise OptionalModuleMissing(['sqlalchemy'],
                                    ("Default database logging requires the sqlalchemy library."
                                     " Enable monitoring support with: pip install parsl[monitoring]"))
    if not _sqlalchemy_utils_enabled:
        raise OptionalModuleMissing(['sqlalchemy_utils'],
                                    ("Default database logging requires the sqlalchemy_utils library."
                                     " Enable monitoring support with: pip install parsl[monitoring]"))

    Base = declarative_base()

    def __init__(self,
                 url='sqlite:///monitoring.db',
                 username=None,
                 password=None,
                 ):

        self.eng = sa.create_engine(url)
        self.meta = self.Base.metadata

        self.meta.create_all(self.eng)
        self.meta.reflect(bind=self.eng)

        Session = sessionmaker(bind=self.eng)
        self.session = Session()

    def update(self, table=None, columns=None, messages=None):
        table = self.meta.tables[table]
        mappings = self._generate_mappings(table, columns=columns,
                                           messages=messages)
        mapper = get_mapper(table)
        self.session.bulk_update_mappings(mapper, mappings)
        self.session.commit()

    def insert(self, table=None, messages=None):
        table = self.meta.tables[table]
        mappings = self._generate_mappings(table, messages=messages)
        mapper = get_mapper(table)
        self.session.bulk_insert_mappings(mapper, mappings)
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def _generate_mappings(self, table, columns=None, messages=[]):
        mappings = []
        for msg in messages:
            m = {}
            if columns is None:
                columns = table.c.keys()
            for column in columns:
                m[column] = msg.get(column, None)
            mappings.append(m)
        return mappings

    class Workflow(Base):
        __tablename__ = WORKFLOW
        run_id = Column(Text, nullable=False, primary_key=True)
        workflow_name = Column(Text, nullable=True)
        workflow_version = Column(Text, nullable=True)
        time_began = Column(DateTime, nullable=False)
        time_completed = Column(DateTime, nullable=True)
        workflow_duration = Column(Float, nullable=True)
        host = Column(Text, nullable=False)
        user = Column(Text, nullable=False)
        rundir = Column(Text, nullable=False)
        tasks_failed_count = Column(Integer, nullable=False)
        tasks_completed_count = Column(Integer, nullable=False)

    # TODO: expand to full set of info
    class Status(Base):
        __tablename__ = STATUS
        task_id = Column(Integer, sa.ForeignKey(
            'task.task_id'), nullable=False)
        task_status_name = Column(Text, nullable=False)
        timestamp = Column(DateTime, nullable=False)
        run_id = Column(Text, sa.ForeignKey('workflow.run_id'), nullable=False)
        hostname = Column('hostname', Text, nullable=True)
        __table_args__ = (
            PrimaryKeyConstraint('task_id', 'run_id',
                                 'task_status_name', 'timestamp'),
        )

    class Task(Base):
        __tablename__ = TASK
        task_id = Column('task_id', Integer, nullable=False)
        run_id = Column('run_id', Text, nullable=False)
        hostname = Column('hostname', Text, nullable=True)
        task_depends = Column('task_depends', Text, nullable=True)
        task_executor = Column('task_executor', Text, nullable=False)
        task_func_name = Column('task_func_name', Text, nullable=False)
        task_time_submitted = Column(
            'task_time_submitted', DateTime, nullable=True)
        task_time_running = Column(
            'task_time_running', DateTime, nullable=True)
        task_time_returned = Column(
            'task_time_returned', DateTime, nullable=True)
        task_memoize = Column('task_memoize', Text, nullable=False)
        task_hashsum = Column('task_hashsum', Text, nullable=True)
        task_inputs = Column('task_inputs', Text, nullable=True)
        task_outputs = Column('task_outputs', Text, nullable=True)
        task_stdin = Column('task_stdin', Text, nullable=True)
        task_stdout = Column('task_stdout', Text, nullable=True)
        task_stderr = Column('task_stderr', Text, nullable=True)
        task_fail_count = Column('task_fail_count', Integer, nullable=False)
        task_fail_history = Column('task_fail_history', Text, nullable=True)
        __table_args__ = (
            PrimaryKeyConstraint('task_id', 'run_id'),
        )

    class Node(Base):
        __tablename__ = NODE
        id = Column('id', Integer, nullable=False, primary_key=True, autoincrement=True)
        run_id = Column('run_id', Text, nullable=False)
        hostname = Column('hostname', Text, nullable=False)
        cpu_count = Column('cpu_count', Integer, nullable=False)
        total_memory = Column('total_memory', Integer, nullable=False)
        active = Column('active', Boolean, nullable=False)
        worker_count = Column('worker_count', Integer, nullable=False)
        python_v = Column('python_v', Text, nullable=False)
        reg_time = Column('reg_time', DateTime, nullable=False)

    class Resource(Base):
        __tablename__ = RESOURCE
        task_id = Column('task_id', Integer, sa.ForeignKey(
            'task.task_id'), nullable=False)
        timestamp = Column('timestamp', DateTime, nullable=False)
        run_id = Column('run_id', Text, sa.ForeignKey(
            'workflow.run_id'), nullable=False)
        resource_monitoring_interval = Column(
            'resource_monitoring_interval', Float, nullable=True)
        psutil_process_pid = Column(
            'psutil_process_pid', Integer, nullable=True)
        psutil_process_cpu_percent = Column(
            'psutil_process_cpu_percent', Float, nullable=True)
        psutil_process_memory_percent = Column(
            'psutil_process_memory_percent', Float, nullable=True)
        psutil_process_children_count = Column(
            'psutil_process_children_count', Float, nullable=True)
        psutil_process_time_user = Column(
            'psutil_process_time_user', Float, nullable=True)
        psutil_process_time_system = Column(
            'psutil_process_time_system', Float, nullable=True)
        psutil_process_memory_virtual = Column(
            'psutil_process_memory_virtual', Float, nullable=True)
        psutil_process_memory_resident = Column(
            'psutil_process_memory_resident', Float, nullable=True)
        psutil_process_disk_read = Column(
            'psutil_process_disk_read', Float, nullable=True)
        psutil_process_disk_write = Column(
            'psutil_process_disk_write', Float, nullable=True)
        psutil_process_status = Column(
            'psutil_process_status', Text, nullable=True)
        __table_args__ = (
            PrimaryKeyConstraint('task_id', 'run_id', 'timestamp'),
        )


class DatabaseManager:
    def __init__(self,
                 db_url='sqlite:///monitoring.db',
                 logdir='.',
                 logging_level=logging.INFO,
                 batching_interval=1,
                 batching_threshold=99999,
                 ):

        self.workflow_end = False
        self.workflow_start_message = None
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        set_file_logger("{}/database_manager.log".format(self.logdir), level=logging_level, name="database_manager")

        logger.debug("Initializing Database Manager process")

        self.db = Database(db_url)
        self.batching_interval = batching_interval
        self.batching_threshold = batching_threshold

        self.pending_priority_queue = queue.Queue()
        self.pending_node_queue = queue.Queue()
        self.pending_resource_queue = queue.Queue()

    def start(self, priority_queue, node_queue, resource_queue):

        self._kill_event = threading.Event()
        self._priority_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                            args=(
                                                                priority_queue, 'priority', self._kill_event,),
                                                            name="Monitoring-migrate-priority",
                                                            daemon=True,
                                                            )
        self._priority_queue_pull_thread.start()

        self._node_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                        args=(
                                                            node_queue, 'node', self._kill_event,),
                                                        name="Monitoring-migrate-node",
                                                        daemon=True,
                                                        )
        self._node_queue_pull_thread.start()

        self._resource_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                            args=(
                                                                resource_queue, 'resource', self._kill_event,),
                                                            name="Monitoring-migrate-resource",
                                                            daemon=True,
                                                            )
        self._resource_queue_pull_thread.start()

        """
        maintain a set to track the tasks that are already INSERTED into database
        to prevent race condition that the first resource message (indicate 'running' state)
        arrives before the first task message.
        If race condition happens, add to left_messages and operate them later

        """
        inserted_tasks = set()
        left_messages = {}

        while (not self._kill_event.is_set() or
               self.pending_priority_queue.qsize() != 0 or self.pending_resource_queue.qsize() != 0 or
               priority_queue.qsize() != 0 or resource_queue.qsize() != 0):

            """
            WORKFLOW_INFO and TASK_INFO messages

            """
            logger.debug("""Checking STOP conditions: {}, {}, {}, {}, {}""".format(
                              self._kill_event.is_set(),
                              self.pending_priority_queue.qsize() != 0, self.pending_resource_queue.qsize() != 0,
                              priority_queue.qsize() != 0, resource_queue.qsize() != 0))

            # This is the list of first resource messages indicating that task starts running
            first_messages = []

            # Get a batch of priority messages
            messages = self._get_messages_in_batch(self.pending_priority_queue,
                                                   interval=self.batching_interval,
                                                   threshold=self.batching_threshold)
            if messages:
                logger.debug(
                    "Got {} messages from priority queue".format(len(messages)))
                update_messages, insert_messages, all_messages = [], [], []
                for msg_type, msg in messages:
                    if msg_type.value == MessageType.WORKFLOW_INFO.value:
                        if "python_version" in msg:   # workflow start message
                            logger.debug(
                                "Inserting workflow start info to WORKFLOW table")
                            self._insert(table=WORKFLOW, messages=[msg])
                            self.workflow_start_message = msg
                        else:                         # workflow end message
                            logger.debug(
                                "Updating workflow end info to WORKFLOW table")
                            self._update(table=WORKFLOW,
                                         columns=['run_id', 'tasks_failed_count',
                                                  'tasks_completed_count', 'time_completed',
                                                  'workflow_duration'],
                                         messages=[msg])
                            self.workflow_end = True

                    else:                             # TASK_INFO message
                        all_messages.append(msg)
                        if msg['task_id'] in inserted_tasks:
                            update_messages.append(msg)
                        else:
                            inserted_tasks.add(msg['task_id'])
                            insert_messages.append(msg)

                            # check if there is an left_message for this task
                            if msg['task_id'] in left_messages:
                                first_messages.append(
                                    left_messages.pop(msg['task_id']))

                logger.debug(
                    "Updating and inserting TASK_INFO to all tables")

                if insert_messages:
                    self._insert(table=TASK, messages=insert_messages)
                    logger.debug(
                        "There are {} inserted task records".format(len(inserted_tasks)))
                if update_messages:
                    self._update(table=WORKFLOW,
                                 columns=['run_id', 'tasks_failed_count',
                                          'tasks_completed_count'],
                                 messages=update_messages)
                    self._update(table=TASK,
                                 columns=['task_time_returned',
                                          'run_id', 'task_id',
                                          'task_fail_count',
                                          'task_fail_history'],
                                 messages=update_messages)
                self._insert(table=STATUS, messages=all_messages)

            """
            NODE_INFO messages

            """
            messages = self._get_messages_in_batch(self.pending_node_queue,
                                                   interval=self.batching_interval,
                                                   threshold=self.batching_threshold)
            if messages:
                logger.debug(
                    "Got {} messages from node queue".format(len(messages)))
                self._insert(table=NODE, messages=messages)

            """
            Resource info messages

            """
            messages = self._get_messages_in_batch(self.pending_resource_queue,
                                                   interval=self.batching_interval,
                                                   threshold=self.batching_threshold)

            if messages or first_messages:
                logger.debug(
                    "Got {} messages from resource queue".format(len(messages)))
                self._insert(table=RESOURCE, messages=messages)
                for msg in messages:
                    if msg['first_msg']:
                        msg['task_status_name'] = States.running.name
                        msg['task_time_running'] = msg['timestamp']
                        if msg['task_id'] in inserted_tasks:
                            first_messages.append(msg)
                        else:
                            left_messages[msg['task_id']] = msg
                if first_messages:
                    self._insert(table=STATUS, messages=first_messages)
                    self._update(table=TASK,
                                 columns=['task_time_running',
                                          'run_id', 'task_id',
                                          'hostname'],
                                 messages=first_messages)

    def _migrate_logs_to_internal(self, logs_queue, queue_tag, kill_event):
        logger.info("[{}_queue_PULL_THREAD] Starting".format(queue_tag))

        while not kill_event.is_set() or logs_queue.qsize() != 0:
            logger.debug("""Checking STOP conditions for {} threads: {}, {}"""
                         .format(queue_tag, kill_event.is_set(), logs_queue.qsize() != 0))
            try:
                x, addr = logs_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            else:
                if queue_tag == 'priority':
                    if x == 'STOP':
                        self.close()
                    else:
                        self.pending_priority_queue.put(x)
                elif queue_tag == 'resource':
                    self.pending_resource_queue.put(x[-1])
                elif queue_tag == 'node':
                    self.pending_node_queue.put(x[-1])

    def _update(self, table, columns, messages):
        try:
            self.db.update(table=table, columns=columns, messages=messages)
        except KeyboardInterrupt:
            logger.exception("KeyboardInterrupt when trying to update Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")
            raise
        except Exception:
            logger.exception("Got exception when trying to update Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")

    def _insert(self, table, messages):
        try:
            self.db.insert(table=table, messages=messages)
        except KeyboardInterrupt:
            logger.exception("KeyboardInterrupt when trying to update Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")
            raise
        except Exception:
            logger.exception("Got exception when trying to insert to Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")

    def _get_messages_in_batch(self, msg_queue, interval=1, threshold=99999):
        messages = []
        start = time.time()
        while True:
            if time.time() - start >= interval or len(messages) >= threshold:
                break
            try:
                x = msg_queue.get(timeout=0.1)
                # logger.debug("Database manager receives a message {}".format(x))
            except queue.Empty:
                logger.debug("Database manager has not received any message.")
                break
            else:
                messages.append(x)
        return messages

    def close(self):
        logger.info("Database Manager cleanup initiated.")
        if not self.workflow_end and self.workflow_start_message:
            logger.info("Logging workflow end info to database due to abnormal exit")
            time_completed = datetime.datetime.now()
            msg = {'time_completed': time_completed,
                   'workflow_duration': (time_completed - self.workflow_start_message['time_began']).total_seconds()}
            self.workflow_start_message.update(msg)
            self._update(table=WORKFLOW,
                         columns=['run_id', 'time_completed',
                                  'workflow_duration'],
                         messages=[self.workflow_start_message])
        self.batching_interval, self.batching_threshold = float(
            'inf'), float('inf')
        self._kill_event.set()


def dbm_starter(exception_q, priority_msgs, node_msgs, resource_msgs, *args, **kwargs):
    """Start the database manager process

    The DFK should start this function. The args, kwargs match that of the monitoring config

    """
    try:
        dbm = DatabaseManager(*args, **kwargs)
        logger.info("Starting dbm in dbm starter")
        dbm.start(priority_msgs, node_msgs, resource_msgs)
    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt signal caught")
        dbm.close()
        raise
    except Exception as e:
        logger.exception("dbm.start exception")
        exception_q.put(("DBM", str(e)))
        dbm.close()

    logger.info("End of dbm_starter")
