import logging
import time
import json

logger = logging.getLogger(__name__)

# Try to get rid of streamed loggers
root_logger = logging.getLogger()
root_logger.addHandler(logging.NullHandler())

try:
    from tornado import httpclient
    import sqlalchemy as sa
    from sqlalchemy import Table, Column, Text
except ImportError:
    pass


# TODO: expand to full set of info
def create_workflows_table(meta):
    return Table(
            'workflows', meta,
            Column('run_id', Text, nullable=False, primary_key=True),
            Column('workflow_name', Text, nullable=False),
            Column('workflow_version', Text, nullable=False),
            Column('time_began', Text, nullable=False),
            Column('time_completed', Text),
            Column('host', Text, nullable=False),
            Column('user', Text, nullable=False),
            Column('rundir', Text, nullable=False),
            Column('tasks_failed_count', Text, nullable=False),
            Column('tasks_completed_count', Text, nullable=False),
    )


# TODO: expand to full set of info
def create_task_status_table(meta):
    return Table(
          'task_status', meta,
          Column('task_id', Text, sa.ForeignKey('task.task_id'), nullable=False),
          Column('task_status', Text, nullable=False),
          Column('task_status_name', Text, nullable=False),
          Column('timestamp', Text, nullable=False),
          Column('run_id', Text, sa.ForeignKey('workflows.run_id'), nullable=False),
          Column('task_fail_count', Text, nullable=False),
          Column('task_fail_history', Text, nullable=True),
    )


def create_task_table(meta):
    return Table(
          'task', meta,
          Column('task_id', Text, nullable=False),
          Column('run_id', Text, sa.ForeignKey('workflows.run_id'), nullable=False),
          Column('task_executor', Text, nullable=False),
          Column('task_fn_hash', Text, nullable=False),
          Column('task_time_started', Text, nullable=False),
          Column('task_time_completed', Text, nullable=True),
          Column('task_memoize', Text, nullable=False),
          Column('task_inputs', Text, nullable=True),
          Column('task_outputs', Text, nullable=True),
          Column('task_stdin', Text, nullable=True),
          Column('task_stdout', Text, nullable=True),
    )


def create_task_resource_table(meta):
    return Table(
          'task_resources', meta,
          Column('task_id', Text, sa.ForeignKey('task.task_id'), nullable=False),
          Column('timestamp', Text, nullable=False),
          Column('run_id', Text, sa.ForeignKey('workflows.run_id'), nullable=False),
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


class DatabaseHandler(logging.Handler):
    """ The handler that takes a log record and puts it into an SQL database. Needs to be a bit fast. """
    def __init__(self, elink):
        """ Set up the handler to link it to the database specified by elink. """
        logging.Handler.__init__(self)
        self.eng = sa.create_engine(elink)
        self.meta = sa.MetaData()
        self.meta.reflect(bind=self.eng)

    def emit(self, record):
        """ Take a task record and insert or update information in the SQL database. Creates the necessary tables if needed.
        Allow multiple tries since originally this was erroneous and used concurrently. Squashes errors/exceptions.
        This handler is added to a logger that is returned by get_db_logger when requested. """
        self.eng.dispose()
        trys = 3
        info = {key: value for key, value in record.__dict__.items() if not key.startswith("__")}
        for t in range(trys):
            # Having what i think is an issue to reflect so try a couple times and don't complain if breaks
            failed = False
            try:
                with self.eng.connect() as con:
                    # formating values to convert from python or parsl to db standards
                    info['task_fail_history'] = str(info.get('task_fail_history', None))
                    info['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created))
                    run_id = info['run_id']

                    # if workflow or task has completed, update their entries with the time.
                    if 'time_completed' in info.keys() and info['time_completed'] != 'None':
                        workflows = self.meta.tables['workflows']
                        up = workflows.update().values(time_completed=info['time_completed']).where(workflows.c.run_id == run_id)
                        con.execute(up)
                        return
                    if 'task_time_completed' in info.keys() and info['task_time_completed'] is not None:
                        workflow = self.meta.tables['task']
                        up = workflow.update().values(task_time_completed=info['task_time_completed']).where(workflow.c.task_id == info['task_id'])\
                                     .where(workflow.c.run_id == run_id)
                        con.execute(up)

                    # create workflows table if this is a new database without one
                    if 'workflows' not in self.meta.tables.keys():
                        workflows = create_workflows_table(self.meta)
                        self.meta.create_all(con)
                    # if this is the first sight of the workflow, add it to the workflows table
                    if len(con.execute(self.meta.tables['workflows'].select(self.meta.tables['workflows'].c.run_id == run_id)).fetchall()) == 0:
                        workflows = self.meta.tables['workflows']
                        ins = workflows.insert().values(**{k: v for k, v in info.items() if k in workflows.c})
                        con.execute(ins)

                    # if log has task counts, update the workflow entry in the workflows table
                    if 'tasks_completed_count' in info.keys():
                        workflows = self.meta.tables['workflows']
                        up = workflows.update().values(tasks_completed_count=info['tasks_completed_count']).where(workflows.c.run_id == run_id)
                        con.execute(up)
                    if 'tasks_failed_count' in info.keys():
                        workflows = self.meta.tables['workflows']
                        up = workflows.update().values(tasks_failed_count=info['tasks_failed_count']).where(workflows.c.run_id == run_id)
                        con.execute(up)

                    # create task table if this is a new run without one
                    if 'task' not in self.meta.tables.keys():
                        workflow = create_task_table(self.meta)
                        self.meta.create_all(con)

                    # check to make sure it is a task log and not just a workflow overview log
                    if info.get('task_id', None) is not None:
                        # if this is the first sight of the task in the workflow, add it to the workflow table
                        if len(con.execute(self.meta.tables['task'].select(self.meta.tables['task'].c.task_id == info['task_id'])
                                               .where(self.meta.tables['task'].c.run_id == run_id)).fetchall()) == 0:
                            if 'psutil_process_pid' in info.keys():
                                # this is the race condition that a resource log is before a status log so ignore this resource update
                                return

                            workflow = self.meta.tables['task']
                            ins = workflow.insert().values(**{k: v for k, v in info.items() if k in workflow.c})
                            con.execute(ins)

                        if 'task_status' in info.keys():
                            # if this is the first sight of a task, create a task_status_table to hold this task's updates
                            if 'task_status' not in self.meta.tables.keys():
                                task_status_table = create_task_status_table(self.meta)
                                self.meta.create_all(con)
                                con.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                            # if this status table already exists, just insert the update
                            else:
                                task_status_table = self.meta.tables['task_status']
                                con.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                            return

                        if 'psutil_process_pid' in info.keys():
                            # if this is a task resource update then handle that, if the resource table DNE then create it
                            if 'task_resources' not in self.meta.tables.keys():
                                task_resource_table = create_task_resource_table(self.meta)
                                self.meta.create_all(con)
                                con.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                            # if this resource table already exists, just insert the update
                            else:
                                task_resource_table = self.meta.tables['task_resources']
                                con.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                            return

            except Exception as e:
                logger.error("Try a couple times since some known issues can occur. Number of Failures: {} Error: {}".format(t, str(e)))
                failed = True
                time.sleep(5)
            if not failed:
                return


class RemoteHandler(logging.Handler):
    """ Handler used to pass a log to the logging server for it to be written to the database.
    This handler is added to a logger that is returned by get_db_logger when requested. """
    def __init__(self, web_app_host, web_app_port, request_timeout=40, retries=3, on_fail_sleep_duration=5):
        """ Set up various behavior of the attempt to connect to the logging server. Not currently configurable. """
        logging.Handler.__init__(self)
        self.addr = web_app_host + ':' + str(web_app_port)
        self.request_timeout = request_timeout
        self.retries = retries
        self.on_fail_sleep_duration = on_fail_sleep_duration
        self.http_client = httpclient.HTTPClient()

    def emit(self, record):
        """ Open up an HTTP connection to the logging server. This is currently blocking.
        It should not matter for the resource monitors but could hold up the DFK logs if this connection/sending takes a lot of time. """
        standard_log_info = ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename', 'module', 'exc_info', 'exc_text', 'stack_info', 'lineno',
                             'funcName', 'created', 'msecs', 'relativeCreated', 'thread', 'threadName', 'processName', 'process']
        for t in range(self.retries):
            try:
                info = {k: str(v) for k, v in record.__dict__.items() if not k.startswith('__') and k not in standard_log_info}
                bod = 'log={}'.format(json.dumps(info))
                self.http_client.fetch(self.addr, method='POST', body=bod, request_timeout=self.request_timeout)
            except Exception as e:
                # Other errors are possible, such as IOError.
                logger.error(str(e))
                time.sleep(self.on_fail_sleep_duration)
            else:
                break
        # http_client.close()
        return
