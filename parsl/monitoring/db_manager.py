import logging
import threading
import queue
import os
import time
import datetime

from typing import Any, Dict, List, Optional, Set, Tuple, TypeVar, cast

from parsl.log_utils import set_file_logger
from parsl.dataflow.states import States
from parsl.errors import OptionalModuleMissing
from parsl.monitoring.message_type import MessageType
from parsl.monitoring.types import MonitoringMessage, TaggedMonitoringMessage
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger("database_manager")

X = TypeVar('X')

try:
    import sqlalchemy as sa
    from sqlalchemy import Column, Text, Float, Boolean, BigInteger, Integer, DateTime, PrimaryKeyConstraint, Table
    from sqlalchemy.orm import Mapper
    from sqlalchemy.orm import mapperlib
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base
except ImportError:
    _sqlalchemy_enabled = False
else:
    _sqlalchemy_enabled = True


WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
TRY = 'try'              # Try table includes information about each attempt to run a task
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization
NODE = 'node'            # Node table include node info
BLOCK = 'block'          # Block table include the status for block polling


class Database:

    if not _sqlalchemy_enabled:
        raise OptionalModuleMissing(['sqlalchemy'],
                                    ("Monitoring requires the sqlalchemy library."
                                     " Install monitoring dependencies with: pip install 'parsl[monitoring]'"))
    Base = declarative_base()

    def __init__(self,
                 url: str = 'sqlite:///runinfomonitoring.db',
                 ):

        self.eng = sa.create_engine(url)
        self.meta = self.Base.metadata

        # TODO: this code wants a read lock on the sqlite3 database, and fails if it cannot
        # - for example, if someone else is querying the database at the point that the
        # monitoring system is initialized. See PR #1917 for related locked-for-read fixes
        # elsewhere in this file.
        self.meta.create_all(self.eng)

        self.meta.reflect(bind=self.eng)

        Session = sessionmaker(bind=self.eng)
        self.session = Session()

    def _get_mapper(self, table_obj: Table) -> Mapper:
        all_mappers: Set[Mapper] = set()
        for mapper_registry in mapperlib._all_registries():  # type: ignore[attr-defined]
            all_mappers.update(mapper_registry.mappers)
        mapper_gen = (
            mapper for mapper in all_mappers
            if table_obj in mapper.tables
        )
        try:
            mapper = next(mapper_gen)
            second_mapper = next(mapper_gen, False)
        except StopIteration:
            raise ValueError(f"Could not get mapper for table {table_obj}")

        if second_mapper:
            raise ValueError(f"Multiple mappers for table {table_obj}")
        return mapper

    def update(self, *, table: str, columns: List[str], messages: List[MonitoringMessage]) -> None:
        table_obj = self.meta.tables[table]
        mappings = self._generate_mappings(table_obj, columns=columns,
                                           messages=messages)
        mapper = self._get_mapper(table_obj)
        self.session.bulk_update_mappings(mapper, mappings)
        self.session.commit()

    def insert(self, *, table: str, messages: List[MonitoringMessage]) -> None:
        table_obj = self.meta.tables[table]
        mappings = self._generate_mappings(table_obj, messages=messages)
        mapper = self._get_mapper(table_obj)
        self.session.bulk_insert_mappings(mapper, mappings)
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()

    def _generate_mappings(self, table: Table, columns: Optional[List[str]] = None, messages: List[MonitoringMessage] = []) -> List[Dict[str, Any]]:
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
        host = Column(Text, nullable=False)
        user = Column(Text, nullable=False)
        rundir = Column(Text, nullable=False)
        tasks_failed_count = Column(Integer, nullable=False)
        tasks_completed_count = Column(Integer, nullable=False)

    class Status(Base):
        __tablename__ = STATUS
        task_id = Column(Integer, nullable=False)
        task_status_name = Column(Text, nullable=False)
        timestamp = Column(DateTime, nullable=False)
        run_id = Column(Text, sa.ForeignKey('workflow.run_id'), nullable=False)
        try_id = Column('try_id', Integer, nullable=False)
        __table_args__ = (
            PrimaryKeyConstraint('task_id', 'run_id',
                                 'task_status_name', 'timestamp'),
        )

    class Task(Base):
        __tablename__ = TASK
        task_id = Column('task_id', Integer, nullable=False)
        run_id = Column('run_id', Text, nullable=False)
        task_depends = Column('task_depends', Text, nullable=True)
        task_func_name = Column('task_func_name', Text, nullable=False)
        task_memoize = Column('task_memoize', Text, nullable=False)
        task_hashsum = Column('task_hashsum', Text, nullable=True, index=True)
        task_inputs = Column('task_inputs', Text, nullable=True)
        task_outputs = Column('task_outputs', Text, nullable=True)
        task_stdin = Column('task_stdin', Text, nullable=True)
        task_stdout = Column('task_stdout', Text, nullable=True)
        task_stderr = Column('task_stderr', Text, nullable=True)

        task_time_invoked = Column(
            'task_time_invoked', DateTime, nullable=True)

        task_time_returned = Column(
            'task_time_returned', DateTime, nullable=True)

        task_fail_count = Column('task_fail_count', Integer, nullable=False)
        task_fail_cost = Column('task_fail_cost', Float, nullable=False)

        __table_args__ = (
            PrimaryKeyConstraint('task_id', 'run_id'),
        )

    class Try(Base):
        __tablename__ = TRY
        try_id = Column('try_id', Integer, nullable=False)
        task_id = Column('task_id', Integer, nullable=False)
        run_id = Column('run_id', Text, nullable=False)

        block_id = Column('block_id', Text, nullable=True)
        hostname = Column('hostname', Text, nullable=True)

        task_executor = Column('task_executor', Text, nullable=False)

        task_try_time_launched = Column(
            'task_try_time_launched', DateTime, nullable=True)

        task_try_time_running = Column(
            'task_try_time_running', DateTime, nullable=True)

        task_try_time_returned = Column(
            'task_try_time_returned', DateTime, nullable=True)

        task_fail_history = Column('task_fail_history', Text, nullable=True)

        task_joins = Column('task_joins', Text, nullable=True)

        __table_args__ = (
            PrimaryKeyConstraint('try_id', 'task_id', 'run_id'),
        )

    class Node(Base):
        __tablename__ = NODE
        id = Column('id', Integer, nullable=False, primary_key=True, autoincrement=True)
        run_id = Column('run_id', Text, nullable=False)
        hostname = Column('hostname', Text, nullable=False)
        uid = Column('uid', Text, nullable=False)
        block_id = Column('block_id', Text, nullable=False)
        cpu_count = Column('cpu_count', Integer, nullable=False)
        total_memory = Column('total_memory', BigInteger, nullable=False)
        active = Column('active', Boolean, nullable=False)
        worker_count = Column('worker_count', Integer, nullable=False)
        python_v = Column('python_v', Text, nullable=False)
        timestamp = Column('timestamp', DateTime, nullable=False)
        last_heartbeat = Column('last_heartbeat', DateTime, nullable=False)

    class Block(Base):
        __tablename__ = BLOCK
        run_id = Column('run_id', Text, nullable=False)
        executor_label = Column('executor_label', Text, nullable=False)
        block_id = Column('block_id', Text, nullable=False)
        job_id = Column('job_id', Text, nullable=True)
        timestamp = Column('timestamp', DateTime, nullable=False)
        status = Column("status", Text, nullable=False)
        __table_args__ = (
            PrimaryKeyConstraint('run_id', 'block_id', 'executor_label', 'timestamp'),
        )

    class Resource(Base):
        __tablename__ = RESOURCE
        try_id = Column('try_id', Integer, nullable=False)
        task_id = Column('task_id', Integer, nullable=False)
        run_id = Column('run_id', Text, sa.ForeignKey(
            'workflow.run_id'), nullable=False)
        timestamp = Column('timestamp', DateTime, nullable=False)
        resource_monitoring_interval = Column(
            'resource_monitoring_interval', Float, nullable=True)
        psutil_process_pid = Column(
            'psutil_process_pid', Integer, nullable=True)
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
            PrimaryKeyConstraint('try_id', 'task_id', 'run_id', 'timestamp'),
        )


class DatabaseManager:
    def __init__(self,
                 db_url: str = 'sqlite:///runinfo/monitoring.db',
                 logdir: str = '.',
                 logging_level: int = logging.INFO,
                 batching_interval: float = 1,
                 batching_threshold: float = 99999,
                 ):

        self.workflow_end = False
        self.workflow_start_message = None  # type: Optional[MonitoringMessage]
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        logger.propagate = False

        set_file_logger("{}/database_manager.log".format(self.logdir), level=logging_level,
                        format_string="%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s] [%(threadName)s %(thread)d] %(message)s",
                        name="database_manager")

        logger.debug("Initializing Database Manager process")

        self.db = Database(db_url)
        self.batching_interval = batching_interval
        self.batching_threshold = batching_threshold

        self.pending_priority_queue = queue.Queue()  # type: queue.Queue[TaggedMonitoringMessage]
        self.pending_node_queue = queue.Queue()  # type: queue.Queue[MonitoringMessage]
        self.pending_block_queue = queue.Queue()  # type: queue.Queue[MonitoringMessage]
        self.pending_resource_queue = queue.Queue()  # type: queue.Queue[MonitoringMessage]

    def start(self,
              priority_queue: "queue.Queue[TaggedMonitoringMessage]",
              node_queue: "queue.Queue[MonitoringMessage]",
              block_queue: "queue.Queue[MonitoringMessage]",
              resource_queue: "queue.Queue[MonitoringMessage]") -> None:

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

        self._block_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                         args=(
                                                             block_queue, 'block', self._kill_event,),
                                                         name="Monitoring-migrate-block",
                                                         daemon=True,
                                                         )
        self._block_queue_pull_thread.start()

        self._resource_queue_pull_thread = threading.Thread(target=self._migrate_logs_to_internal,
                                                            args=(
                                                                resource_queue, 'resource', self._kill_event,),
                                                            name="Monitoring-migrate-resource",
                                                            daemon=True,
                                                            )
        self._resource_queue_pull_thread.start()

        """
        maintain a set to track the tasks that are already INSERTed into database
        to prevent race condition that the first resource message (indicate 'running' state)
        arrives before the first task message. In such a case, the resource table
        primary key would be violated.
        If that happens, the message will be added to deferred_resource_messages and processed later.

        """
        inserted_tasks = set()  # type: Set[object]

        """
        like inserted_tasks but for task,try tuples
        """
        inserted_tries = set()  # type: Set[Any]

        # for any task ID, we can defer exactly one message, which is the
        # assumed-to-be-unique first message (with first message flag set).
        # The code prior to this patch will discard previous message in
        # the case of multiple messages to defer.
        deferred_resource_messages = {}  # type: MonitoringMessage

        exception_happened = False

        while (not self._kill_event.is_set() or
               self.pending_priority_queue.qsize() != 0 or self.pending_resource_queue.qsize() != 0 or
               self.pending_node_queue.qsize() != 0 or self.pending_block_queue.qsize() != 0 or
               priority_queue.qsize() != 0 or resource_queue.qsize() != 0 or
               node_queue.qsize() != 0 or block_queue.qsize() != 0):

            """
            WORKFLOW_INFO and TASK_INFO messages (i.e. priority messages)

            """
            try:
                logger.debug("""Checking STOP conditions: {}, {}, {}, {}, {}, {}, {}, {}, {}""".format(
                                  self._kill_event.is_set(),
                                  self.pending_priority_queue.qsize() != 0, self.pending_resource_queue.qsize() != 0,
                                  self.pending_node_queue.qsize() != 0, self.pending_block_queue.qsize() != 0,
                                  priority_queue.qsize() != 0, resource_queue.qsize() != 0,
                                  node_queue.qsize() != 0, block_queue.qsize() != 0))

                # This is the list of resource messages which can be reprocessed as if they
                # had just arrived because the corresponding first task message has been
                # processed (corresponding by task id)
                reprocessable_first_resource_messages = []

                # end-of-task-run status messages - handled in similar way as
                # for last resource messages to try to have symmetry... this
                # needs a type annotation though reprocessable_first_resource_messages
                # doesn't... not sure why. Too lazy right now to figure out what,
                # if any, more specific type than Any the messages have.
                reprocessable_last_resource_messages: List[Any] = []

                # Get a batch of priority messages
                priority_messages = self._get_messages_in_batch(self.pending_priority_queue)
                if priority_messages:
                    logger.debug(
                        "Got {} messages from priority queue".format(len(priority_messages)))
                    task_info_update_messages, task_info_insert_messages, task_info_all_messages = [], [], []
                    try_update_messages, try_insert_messages, try_all_messages = [], [], []
                    for msg_type, msg in priority_messages:
                        if msg_type == MessageType.WORKFLOW_INFO:
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
                                                      'tasks_completed_count', 'time_completed'],
                                             messages=[msg])
                                self.workflow_end = True

                        elif msg_type == MessageType.TASK_INFO:
                            task_try_id = str(msg['task_id']) + "." + str(msg['try_id'])
                            task_info_all_messages.append(msg)
                            if msg['task_id'] in inserted_tasks:
                                task_info_update_messages.append(msg)
                            else:
                                inserted_tasks.add(msg['task_id'])
                                task_info_insert_messages.append(msg)

                            try_all_messages.append(msg)
                            if task_try_id in inserted_tries:
                                try_update_messages.append(msg)
                            else:
                                inserted_tries.add(task_try_id)
                                try_insert_messages.append(msg)

                                # check if there is a left_message for this task
                                if task_try_id in deferred_resource_messages:
                                    reprocessable_first_resource_messages.append(
                                        deferred_resource_messages.pop(task_try_id))
                        else:
                            raise RuntimeError("Unexpected message type {} received on priority queue".format(msg_type))

                    logger.debug("Updating and inserting TASK_INFO to all tables")
                    logger.debug("Updating {} TASK_INFO into workflow table".format(len(task_info_update_messages)))
                    self._update(table=WORKFLOW,
                                 columns=['run_id', 'tasks_failed_count',
                                          'tasks_completed_count'],
                                 messages=task_info_all_messages)

                    if task_info_insert_messages:
                        self._insert(table=TASK, messages=task_info_insert_messages)
                        logger.debug(
                            "There are {} inserted task records".format(len(inserted_tasks)))

                    if task_info_update_messages:
                        logger.debug("Updating {} TASK_INFO into task table".format(len(task_info_update_messages)))
                        self._update(table=TASK,
                                     columns=['task_time_invoked',
                                              'task_time_returned',
                                              'run_id', 'task_id',
                                              'task_fail_count',
                                              'task_fail_cost',
                                              'task_hashsum'],
                                     messages=task_info_update_messages)
                    logger.debug("Inserting {} task_info_all_messages into status table".format(len(task_info_all_messages)))

                    self._insert(table=STATUS, messages=task_info_all_messages)

                    if try_insert_messages:
                        logger.debug("Inserting {} TASK_INFO to try table".format(len(try_insert_messages)))
                        self._insert(table=TRY, messages=try_insert_messages)
                        logger.debug(
                            "There are {} inserted task records".format(len(inserted_tasks)))

                    if try_update_messages:
                        logger.debug("Updating {} TASK_INFO into try table".format(len(try_update_messages)))
                        self._update(table=TRY,
                                     columns=['run_id', 'task_id', 'try_id',
                                              'task_fail_history',
                                              'task_try_time_launched',
                                              'task_try_time_returned',
                                              'task_joins'],
                                     messages=try_update_messages)

                """
                NODE_INFO messages

                """
                node_info_messages = self._get_messages_in_batch(self.pending_node_queue)
                if node_info_messages:
                    logger.debug(
                        "Got {} messages from node queue".format(len(node_info_messages)))
                    self._insert(table=NODE, messages=node_info_messages)

                """
                BLOCK_INFO messages

                """
                block_info_messages = self._get_messages_in_batch(self.pending_block_queue)
                if block_info_messages:
                    logger.debug(
                        "Got {} messages from block queue".format(len(block_info_messages)))
                    # block_info_messages is possibly a nested list of dict (at different polling times)
                    # Each dict refers to the info of a job/block at one polling time
                    block_messages_to_insert = []  # type: List[Any]
                    for block_msg in block_info_messages:
                        block_messages_to_insert.extend(block_msg)
                    self._insert(table=BLOCK, messages=block_messages_to_insert)

                """
                Resource info messages

                """
                resource_messages = self._get_messages_in_batch(self.pending_resource_queue)

                if resource_messages:
                    logger.debug(
                        "Got {} messages from resource queue, "
                        "{} reprocessable as first messages, "
                        "{} reprocessable as last messages".format(len(resource_messages),
                                                                   len(reprocessable_first_resource_messages),
                                                                   len(reprocessable_last_resource_messages)))

                    insert_resource_messages = []
                    for msg in resource_messages:
                        task_try_id = str(msg['task_id']) + "." + str(msg['try_id'])
                        if msg['first_msg']:
                            # Update the running time to try table if first message
                            msg['task_status_name'] = States.running.name
                            msg['task_try_time_running'] = msg['timestamp']

                            if task_try_id in inserted_tries:  # TODO: needs to become task_id and try_id, and check against inserted_tries
                                reprocessable_first_resource_messages.append(msg)
                            else:
                                if task_try_id in deferred_resource_messages:
                                    logger.error("Task {} already has a deferred resource message. Discarding previous message.".format(msg['task_id']))
                                deferred_resource_messages[task_try_id] = msg
                        elif msg['last_msg']:
                            # This assumes that the primary key has been added
                            # to the try table already, so doesn't use the same
                            # deferral logic as the first_msg case.
                            msg['task_status_name'] = States.running_ended.name
                            reprocessable_last_resource_messages.append(msg)
                        else:
                            # Insert to resource table if not first/last (start/stop) message message
                            insert_resource_messages.append(msg)

                    if insert_resource_messages:
                        self._insert(table=RESOURCE, messages=insert_resource_messages)

                if reprocessable_first_resource_messages:
                    self._insert(table=STATUS, messages=reprocessable_first_resource_messages)
                    self._update(table=TRY,
                                 columns=['task_try_time_running',
                                          'run_id', 'task_id', 'try_id',
                                          'block_id', 'hostname'],
                                 messages=reprocessable_first_resource_messages)

                if reprocessable_last_resource_messages:
                    self._insert(table=STATUS, messages=reprocessable_last_resource_messages)
            except Exception:
                logger.exception("Exception in db loop: this might have been a malformed message, or some other error. monitoring data may have been lost")
                exception_happened = True
        if exception_happened:
            raise RuntimeError("An exception happened sometime during database processing and should have been logged in database_manager.log")

    @wrap_with_logs(target="database_manager")
    def _migrate_logs_to_internal(self, logs_queue: queue.Queue, queue_tag: str, kill_event: threading.Event) -> None:
        logger.info("Starting processing for queue {}".format(queue_tag))

        while not kill_event.is_set() or logs_queue.qsize() != 0:
            logger.debug("""Checking STOP conditions for {} threads: {}, {}"""
                         .format(queue_tag, kill_event.is_set(), logs_queue.qsize() != 0))
            try:
                x, addr = logs_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            else:
                if queue_tag == 'priority' and x == 'STOP':
                    self.close()
                elif queue_tag == 'priority':  # implicitly not 'STOP'
                    assert isinstance(x, tuple)
                    assert len(x) == 2
                    assert x[0] in [MessageType.WORKFLOW_INFO, MessageType.TASK_INFO], \
                        "_migrate_logs_to_internal can only migrate WORKFLOW_,TASK_INFO message from priority queue, got x[0] == {}".format(x[0])
                    self._dispatch_to_internal(x)
                elif queue_tag == 'resource':
                    assert isinstance(x, tuple), "_migrate_logs_to_internal was expecting a tuple, got {}".format(x)
                    assert x[0] == MessageType.RESOURCE_INFO, \
                        "_migrate_logs_to_internal can only migrate RESOURCE_INFO message from resource queue, got tag {}, message {}".format(x[0], x)
                    self._dispatch_to_internal(x)
                elif queue_tag == 'node':
                    assert len(x) == 2, "expected message tuple to have exactly two elements"
                    assert x[0] == MessageType.NODE_INFO, "_migrate_logs_to_internal can only migrate NODE_INFO messages from node queue"

                    self._dispatch_to_internal(x)
                elif queue_tag == "block":
                    self._dispatch_to_internal(x)
                else:
                    logger.error(f"Discarding because unknown queue tag '{queue_tag}', message: {x}")

    def _dispatch_to_internal(self, x: Tuple) -> None:
        if x[0] in [MessageType.WORKFLOW_INFO, MessageType.TASK_INFO]:
            self.pending_priority_queue.put(cast(Any, x))
        elif x[0] == MessageType.RESOURCE_INFO:
            body = x[1]
            self.pending_resource_queue.put(body)
        elif x[0] == MessageType.NODE_INFO:
            assert len(x) == 2, "expected NODE_INFO tuple to have exactly two elements"

            logger.info("Will put {} to pending node queue".format(x[1]))
            self.pending_node_queue.put(x[1])
        elif x[0] == MessageType.BLOCK_INFO:
            logger.info("Will put {} to pending block queue".format(x[1]))
            self.pending_block_queue.put(x[-1])
        else:
            logger.error("Discarding message of unknown type {}".format(x[0]))

    def _update(self, table: str, columns: List[str], messages: List[MonitoringMessage]) -> None:
        try:
            done = False
            while not done:
                try:
                    self.db.update(table=table, columns=columns, messages=messages)
                    done = True
                except sa.exc.OperationalError as e:
                    # This code assumes that an OperationalError is something that will go away eventually
                    # if retried - for example, the database being locked because someone else is readying
                    # the tables we are trying to write to. If that assumption is wrong, then this loop
                    # may go on forever.
                    logger.warning("Got a database OperationalError. Ignoring and retrying on the assumption that it is recoverable: {}".format(e))
                    self.db.rollback()
                    time.sleep(1)  # hard coded 1s wait - this should be configurable or exponential backoff or something

        except KeyboardInterrupt:
            logger.exception("KeyboardInterrupt when trying to update Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")
            raise
        except Exception:
            logger.exception("Got exception when trying to update table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")

    def _insert(self, table: str, messages: List[MonitoringMessage]) -> None:
        try:
            done = False
            while not done:
                try:
                    self.db.insert(table=table, messages=messages)
                    done = True
                except sa.exc.OperationalError as e:
                    # hoping that this is a database locked error during _update, not some other problem
                    logger.warning("Got a database OperationalError. Ignoring and retrying on the assumption that it is recoverable: {}".format(e))
                    self.db.rollback()
                    time.sleep(1)  # hard coded 1s wait - this should be configurable or exponential backoff or something
        except KeyboardInterrupt:
            logger.exception("KeyboardInterrupt when trying to update Table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")
            raise
        except Exception:
            logger.exception("Got exception when trying to insert to table {}".format(table))
            try:
                self.db.rollback()
            except Exception:
                logger.exception("Rollback failed")

    def _get_messages_in_batch(self, msg_queue: "queue.Queue[X]") -> List[X]:
        messages = []  # type: List[X]
        start = time.time()
        while True:
            if time.time() - start >= self.batching_interval or len(messages) >= self.batching_threshold:
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

    def close(self) -> None:
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
        self.batching_interval = float('inf')
        self.batching_threshold = float('inf')
        self._kill_event.set()


@wrap_with_logs(target="database_manager")
def dbm_starter(exception_q: "queue.Queue[Tuple[str, str]]",
                priority_msgs: "queue.Queue[TaggedMonitoringMessage]",
                node_msgs: "queue.Queue[MonitoringMessage]",
                block_msgs: "queue.Queue[MonitoringMessage]",
                resource_msgs: "queue.Queue[MonitoringMessage]",
                db_url: str,
                logdir: str,
                logging_level: int) -> None:
    """Start the database manager process

    The DFK should start this function. The args, kwargs match that of the monitoring config

    """
    setproctitle("parsl: monitoring database")

    try:
        dbm = DatabaseManager(db_url=db_url,
                              logdir=logdir,
                              logging_level=logging_level)
        logger.info("Starting dbm in dbm starter")
        dbm.start(priority_msgs, node_msgs, block_msgs, resource_msgs)
    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt signal caught")
        dbm.close()
        raise
    except Exception as e:
        logger.exception("dbm.start exception")
        exception_q.put(("DBM", str(e)))
        dbm.close()

    logger.info("End of dbm_starter")
