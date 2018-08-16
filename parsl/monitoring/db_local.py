import logging
from logging import Handler
import sqlalchemy as sa
from sqlalchemy import Table, Column, Text, Integer, Float, Boolean


# TODO: expand to full set of info
def create_workflows_table(meta):
    return Table(
            'workflows', meta,
            Column('task_run_id', Text, nullable=False, primary_key=True),
            Column('time_began', Text, nullable=False),
            Column('time_completed', Text),
            # Column('host', Text, nullable=False),
            # Column('user', Text, nullable=False),
            Column('rundir', Text, nullable=False)
    )


# TODO: expand to full set of info
def create_task_status_table(task_id, run_id, meta):
    table_name = run_id + str(task_id)
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


# TODO: expand to full set of info
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
          # Column('task_kwargs.input', Text, nullable=True),
          # Column('task_kwargs.output', Text, nullable=True),
          # Column('task_kwargs.stdin', Text, nullable=True),
          # Column('task_kwargs.stdout', Text, nullable=True),
    )


# TODO: expand to full set of info
def create_task_resource_table(task_id, run_id, meta):
    table_name = run_id + str(task_id)
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
        self.meta = sa.MetaData()
        self.meta.reflect(bind=self.eng)

    def emit(self, record):
        # self.meta.reflect(bind=self.eng)
        info = {key: value for key, value in record.__dict__.items() if not key.startswith("__")}
        # formating values to convert from python or parsl to db standards
        info = ', '.join(info['task_fail_history']) if info.get('task_fail_history', None) is not None else None
        info['timestamp'] = record.created
        run_id = info['task_run_id']
        # create workflows table if this is a new database without one
        if 'workflows' not in self.meta.tables.keys():
            workflows = create_workflows_table(self.meta)
            self.meta.create_all(self.eng)
        # if this is the first sight of the workflow, add it to the workflows table
        if len(self.eng.execute(self.meta.tables['workflows'].select(self.meta.tables['workflows'].c.task_run_id == run_id)).fetchall()) == 0:
            try:
                with self.eng.begin() as con:
                    workflows = self.meta.tables['workflows']
                    ins = workflows.insert().values(**{k: v for k, v in info.items() if k in workflows.c})
                    con.execute(ins)
                    print(run_id + " was added to the workflows table")
            except sa.exc.IntegrityError as e:
                print(e)
                print(dir(e))

        # create workflow table if this is a new run without one
        if run_id not in self.meta.tables.keys():
            workflow = create_workflow_table(run_id, self.meta)
            self.meta.create_all(self.eng)

        # check to make sure it is a task log and not just a workflow overview log
        if info.get('task_id', None) is not None:
            # if this is the first sight of the task in the workflow, add it to the workflow table
            if len(self.eng.execute(self.meta.tables[run_id].select(self.meta.tables[run_id].c.task_id == info['task_id'])).fetchall()) == 0:
                with self.eng.begin() as con:
                    workflow = self.meta.tables[run_id]
                    ins = workflow.insert().values(**{k: v for k, v in info.items() if k in workflow.c})
                    con.execute(ins)
                    print('Task ' + str(info['task_id']) + " was added to the workflow table")

            if 'task_status' in info.keys():
                # TODO: only fire this if it is a task status update and not a task resource update
                # if this is the first sight of a task, create a task_status_table to hold this task's updates
                if (run_id + str(info['task_id'])) not in self.meta.tables.keys():
                    task_status_table = create_task_status_table(info['task_id'], run_id, self.meta)
                    # task_status_table.create(con)
                    self.meta.create_all(self.eng)
                    self.eng.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                    print(task_status_table, 'table was created and had a task status update added')
                # if this status table already exists, just insert the update
            else:
                task_status_table = self.meta.tables[run_id + str(info['task_id'])]
                self.eng.execute(task_status_table.insert().values(**{k: v for k, v in info.items() if k in task_status_table.c}))
                print(task_status_table, 'had a task status update added')

            if 'cpu_percent' in info.keys():
                # TODO: only use this if it is a task resource update and not a task status update
                # if this is a task resource update then handle that, if the resource table DNE then create it
                if (run_id + str(info['task_id']) + "_resources") not in self.meta.tables.keys():
                    task_resource_table = create_task_resource_table(info['task_id'], run_id, self.meta)
                    # task_status_table.create(con)
                    self.meta.create_all(self.eng)
                    self.eng.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                    print(task_resource_table, 'table was created and had a task resource update added')
                # if this resource table already exists, just insert the update
            else:
                task_resource_table = self.meta.tables[run_id + str(info['task_id']) + '_resources']
                self.eng.execute(task_resource_table.insert().values(**{k: v for k, v in info.items() if k in task_resource_table.c}))
                print(task_resource_table, 'had a task resource update added')
