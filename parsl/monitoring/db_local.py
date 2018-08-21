import logging
from logging import Handler
import time

try:
    import sqlalchemy as sa
    from sqlalchemy import Table, Column, Text, Integer, Float, Boolean
except ImportError:
    pass


# TODO: expand to full set of info
def create_workflows_table(meta):
    return Table(
            'workflows', meta,
            Column('task_run_id', Text, nullable=False, primary_key=True),
            Column('time_began', Text, nullable=False),
            Column('time_completed', Text),
            # Column('host', Text, nullable=False),
            # Column('user', Text, nullable=False),
            Column('rundir', Text, nullable=False),
            Column('tasks_failed_count', Integer, nullable=False),
            Column('tasks_completed_count', Integer, nullable=False),
    )


# TODO: expand to full set of info
def create_task_status_table(task_id, run_id, meta):
    table_name = run_id + "-" + str(task_id)
    return Table(
          table_name, meta,
          Column('task_id', Integer, sa.ForeignKey(run_id + '.task_id'), nullable=False),
          Column('task_status', Integer, nullable=False),
          Column('task_status_name', Integer, nullable=False),
          Column('timestamp', Text, nullable=False, primary_key=True),
          Column('task_run_id', Text, sa.ForeignKey('workflows.task_run_id'), nullable=False),
          Column('task_fail_count', Integer, nullable=False),
          Column('task_fail_history', Text, nullable=True),
    )


def create_workflow_table(run_id, meta):
    table_name = run_id
    return Table(
          table_name, meta,
          Column('task_id', Integer, primary_key=True, nullable=False),
          Column('task_run_id', Text, sa.ForeignKey('workflows.task_run_id'), nullable=False),
          Column('task_executor', Text, nullable=False),
          Column('task_fn_hash', Text, nullable=False),
          Column('task_time_started', Text, nullable=False),
          Column('task_time_completed', Text, nullable=True),
          Column('task_memoize', Boolean, nullable=False),
          Column('task_inputs', Text, nullable=True),
          Column('task_outputs', Text, nullable=True),
          Column('task_stdin', Text, nullable=True),
          Column('task_stdout', Text, nullable=True),
    )


def create_task_resource_table(task_id, run_id, meta):
    table_name = run_id + "-" + str(task_id)
    return Table(
          table_name + '_resources', meta,
          Column('task_id', Integer, sa.ForeignKey(run_id + '.task_id'), nullable=False),
          Column('timestamp', Text, nullable=False, primary_key=True),
          Column('task_run_id', Text, sa.ForeignKey('workflows.task_run_id'), nullable=False),
          Column('psutil_process_pid', Integer, nullable=True),
          Column('psutil_process_cpu_percent', Float, nullable=True),
          Column('psutil_process_memory_percent', Float, nullable=True),
          Column('psutil_process_children_count', Integer, nullable=True),
          Column('psutil_process_time_user', Float, nullable=True),
          Column('psutil_process_time_system', Float, nullable=True),
          Column('psutil_process_memory_virtual', Float, nullable=True),
          Column('psutil_process_memory_resident', Float, nullable=True),
          Column('psutil_process_disk_read', Float, nullable=True),
          Column('psutil_process_disk_write', Float, nullable=True),
          Column('psutil_process_status', Text, nullable=True),
    )


class DatabaseHandler(Handler):
    def __init__(self, elink):
        logging.Handler.__init__(self)
        self.eng = sa.create_engine(elink)

    def emit(self, record):
        self.eng.dispose()
        trys = 3
        info = {key: value for key, value in record.__dict__.items() if not key.startswith("__")}
        for t in range(trys):
            # Having what i think is an issue to reflect so try a couple times and don't complain if breaks
            failed = False
            try:
                with self.eng.connect() as con:
                    meta = sa.MetaData()
                    meta.reflect(bind=con)
                    # formating values to convert from python or parsl to db standards
                    info['task_fail_history'] = str(info.get('task_fail_history', None))
                    info['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created))
                    run_id = info['task_run_id']

                    # if workflow or task has completed, update their entries with the time.
                    # FIXME: This appears to not updated failed tasks.
                    if 'time_completed' in info.keys() and info['time_completed'] != 'None':
                        workflows = meta.tables['workflows']
                        up = workflows.update().values(time_completed=info['time_completed']).where(workflows.c.task_run_id == run_id)
                        con.execute(up)
                        return
                    if 'task_time_completed' in info.keys() and info['task_time_completed'] is not None:
                        workflow = meta.tables[run_id]
                        up = workflow.update().values(task_time_completed=info['task_time_completed']).where(workflow.c.task_id == info['task_id'])
                        con.execute(up)

                    # create workflows table if this is a new database without one
                    if 'workflows' not in meta.tables.keys():
                        workflows = create_workflows_table(meta)
                        workflows.create(con, checkfirst=True)
                    # if this is the first sight of the workflow, add it to the workflows table
                    if len(con.execute(meta.tables['workflows'].select(meta.tables['workflows'].c.task_run_id == run_id)).fetchall()) == 0:
                        workflows = meta.tables['workflows']
                        ins = workflows.insert().values(**{k: v for k, v in info.items() if k in workflows.c})
                        con.execute(ins)

                    # if log has task counts, update the workflow entry in the workflows table
                    if 'tasks_completed_count' in info.keys():
                        workflows = meta.tables['workflows']
                        up = workflows.update().values(tasks_completed_count=info['tasks_completed_count']).where(workflows.c.task_run_id == run_id)
                        con.execute(up)
                    if 'tasks_failed_count' in info.keys():
                        workflows = meta.tables['workflows']
                        up = workflows.update().values(tasks_failed_count=info['tasks_failed_count']).where(workflows.c.task_run_id == run_id)
                        con.execute(up)

                    # create workflow table if this is a new run without one
                    if run_id not in meta.tables.keys():
                        workflow = create_workflow_table(run_id, meta)
                        workflow.create(con, checkfirst=True)

                    # check to make sure it is a task log and not just a workflow overview log
                    if info.get('task_id', None) is not None:
                        # if this is the first sight of the task in the workflow, add it to the workflow table
                        if len(con.execute(meta.tables[run_id].select(meta.tables[run_id].c.task_id == info['task_id'])).fetchall()) == 0:
                            if 'psutil_process_pid' in info.keys():
                                # this is the race condition that a resource log is before a status log so ignore this resource update
                                pass

                            workflow = meta.tables[run_id]
                            ins = workflow.insert().values(**{k: v for k, v in info.items() if k in workflow.c})
                            con.execute(ins)

                        if 'task_status' in info.keys():
                            # if this is the first sight of a task, create a task_status_table to hold this task's updates
                            if (run_id + "-" + str(info['task_id'])) not in meta.tables.keys():
                                task_status_table = create_task_status_table(info['task_id'], run_id, meta)
                                task_status_table.create(con, checkfirst=True)
                                con.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                            # if this status table already exists, just insert the update
                            else:
                                task_status_table = meta.tables[run_id + "-" + str(info['task_id'])]
                                con.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                            return

                        if 'psutil_process_pid' in info.keys():
                            # if this is a task resource update then handle that, if the resource table DNE then create it
                            if (run_id + "-" + str(info['task_id']) + "_resources") not in meta.tables.keys():
                                task_resource_table = create_task_resource_table(info['task_id'], run_id, meta)
                                task_resource_table.create(con, checkfirst=True)
                                con.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                            # if this resource table already exists, just insert the update
                            else:
                                task_resource_table = meta.tables[run_id + "-" + str(info['task_id']) + '_resources']
                                con.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                            return

            except Exception as e:
                failed = True
                self.eng.dispose()
                time.sleep(5)
            if failed:
                pass
            else:
                break
        else:
            return
