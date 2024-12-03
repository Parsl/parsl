from flask_sqlalchemy import SQLAlchemy

WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization
NODE = 'node'            # Node table include node info
FILE = 'file'            # Files table include file info
INPUT_FILE = 'input_file'  # Input files table include input file info
OUTPUT_FILE = 'output_file'  # Output files table include output file info
ENVIRONMENT = 'environment'  # Executor table include executor info
MISC_INFO = 'misc_info'  # Misc info table include misc info

db = SQLAlchemy()


class Workflow(db.Model):
    __tablename__ = WORKFLOW
    run_id = db.Column(db.Text, nullable=False, primary_key=True)
    workflow_name = db.Column(db.Text, nullable=True)
    workflow_version = db.Column(db.Text, nullable=True)
    time_began = db.Column(db.DateTime, nullable=False)  # Why not date?
    time_completed = db.Column(db.DateTime)
    host = db.Column(db.Text, nullable=False)
    user = db.Column(db.Text, nullable=False)
    rundir = db.Column(db.Text, nullable=False)
    tasks_failed_count = db.Column(db.Integer, nullable=False)
    tasks_completed_count = db.Column(db.Integer, nullable=False)


class Node(db.Model):
    __tablename__ = NODE
    id = db.Column('id', db.Integer, nullable=False, primary_key=True, autoincrement=True)
    run_id = db.Column('run_id', db.Text, nullable=False)
    hostname = db.Column('hostname', db.Text, nullable=False)
    cpu_count = db.Column('cpu_count', db.Integer, nullable=False)
    total_memory = db.Column('total_memory', db.Integer, nullable=False)
    active = db.Column('active', db.Boolean, nullable=False)
    worker_count = db.Column('worker_count', db.Integer, nullable=False)
    python_v = db.Column('python_v', db.Text, nullable=False)
    reg_time = db.Column('reg_time', db.DateTime, nullable=False)


# TODO: expand to full set of info
class Status(db.Model):
    __tablename__ = STATUS
    task_id = db.Column(db.Integer, db.ForeignKey(
        'task.task_id'), nullable=False)
    task_status_name = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    run_id = db.Column(db.Text, db.ForeignKey(
        'workflow.run_id'), nullable=False)
    __table_args__ = (
        db.PrimaryKeyConstraint('task_id', 'run_id',
                                'task_status_name', 'timestamp'),
    )


class Task(db.Model):
    __tablename__ = TASK
    task_id = db.Column('task_id', db.Integer, nullable=False)
    run_id = db.Column('run_id', db.Text, nullable=False)
    task_func_name = db.Column('task_func_name', db.Text, nullable=False)
    task_depends = db.Column('task_depends', db.Text, nullable=True)
    task_time_invoked = db.Column(
        'task_time_invoked', db.DateTime, nullable=True)
    task_time_returned = db.Column(
        'task_time_returned', db.DateTime, nullable=True)
    task_memoize = db.Column('task_memoize', db.Text, nullable=False)
    task_inputs = db.Column('task_inputs', db.Text, nullable=True)
    task_outputs = db.Column('task_outputs', db.Text, nullable=True)
    task_stdin = db.Column('task_stdin', db.Text, nullable=True)
    task_stdout = db.Column('task_stdout', db.Text, nullable=True)
    task_stderr = db.Column('task_stderr', db.Text, nullable=True)
    task_environment = db.Column('task_environment', db.Text, nullable=True)
    __table_args__ = (
        db.PrimaryKeyConstraint('task_id', 'run_id'),
    )


class File(db.Model):
    __tablename__ = FILE
    file_name = db.Column('file_name', db.Text, index=True, nullable=False)
    file_id = db.Column('file_id', db.Text, index=True, nullable=False)
    run_id = db.Column('run_id', db.Text, nullable=False)
    task_id = db.Column('task_id', db.Integer, nullable=False)
    try_id = db.Column('try_id', db.Integer, nullable=False)
    timestamp = db.Column('timestamp', db.DateTime, nullable=True)
    size = db.Column('size', db.BigInteger, nullable=True)
    md5sum = db.Column('md5sum', db.Text, nullable=True)
    __table_args__ = (db.PrimaryKeyConstraint('file_id'),)


class Environment(db.Model):
    __tablename__ = ENVIRONMENT
    environment_id = db.Column('environment_id', db.Text, nullable=False)
    run_id = db.Column('run_id', db.Text, nullable=False)
    label = db.Column('label', db.Text, nullable=False)
    address = db.Column('address', db.Text, nullable=True)
    provider = db.Column('provider', db.Text, nullable=True)
    launcher = db.Column('launcher', db.Text, nullable=True)
    worker_init = db.Column('worker_init', db.Text, nullable=True)
    __table_args__ = (db.PrimaryKeyConstraint('environment_id'),)


class InputFile(db.Model):
    __tablename__ = INPUT_FILE
    file_id = db.Column('file_id', db.Text, nullable=False)
    run_id = db.Column('run_id', db.Text, nullable=False)
    task_id = db.Column('task_id', db.Integer, nullable=False)
    try_id = db.Column('try_id', db.Integer, nullable=False)
    __table_args__ = (db.PrimaryKeyConstraint('file_id'),)


class OutputFile(db.Model):
    __tablename__ = OUTPUT_FILE
    file_id = db.Column('file_id', db.Text, nullable=False)
    run_id = db. Column('run_id', db.Text, nullable=False)
    task_id = db.Column('task_id', db.Integer, nullable=False)
    try_id = db.Column('try_id', db.Integer, nullable=False)
    __table_args__ = (db.PrimaryKeyConstraint('file_id'),)


class MiscInfo(db.Model):
    __tablename__ = MISC_INFO
    run_id = db.Column('run_id', db.Text, nullable=False)
    timestamp = db.Column('timestamp', db.DateTime, nullable=False)
    info = db.Column('info', db.Text, nullable=False)
    __table_args__ = (db.PrimaryKeyConstraint('run_id', 'timestamp'),)


class Resource(db.Model):
    __tablename__ = RESOURCE
    task_id = db.Column('task_id', db.Integer, db.ForeignKey(
        'task.task_id'), nullable=False)
    timestamp = db.Column('timestamp', db.DateTime, nullable=False)
    run_id = db.Column('run_id', db.Text, db.ForeignKey(
        'workflow.run_id'), nullable=False)
    resource_monitoring_interval = db.Column(
        'resource_monitoring_interval', db.Float, nullable=True)
    psutil_process_pid = db.Column(
        'psutil_process_pid', db.Integer, nullable=True)
    psutil_process_memory_percent = db.Column(
        'psutil_process_memory_percent', db.Float, nullable=True)
    psutil_process_children_count = db.Column(
        'psutil_process_children_count', db.Float, nullable=True)
    psutil_process_time_user = db.Column(
        'psutil_process_time_user', db.Float, nullable=True)
    psutil_process_time_system = db.Column(
        'psutil_process_time_system', db.Float, nullable=True)
    psutil_process_memory_virtual = db.Column(
        'psutil_process_memory_virtual', db.Float, nullable=True)
    psutil_process_memory_resident = db.Column(
        'psutil_process_memory_resident', db.Float, nullable=True)
    psutil_process_disk_read = db.Column(
        'psutil_process_disk_read', db.Float, nullable=True)
    psutil_process_disk_write = db.Column(
        'psutil_process_disk_write', db.Float, nullable=True)
    psutil_process_status = db.Column(
        'psutil_process_status', db.Text, nullable=True)
    psutil_cpu_num = db.Column(
        'psutil_cpu_num', db.Text, nullable=True)
    psutil_process_num_ctx_switches_voluntary = db.Column(
        'psutil_process_num_ctx_switches_voluntary', db.Float, nullable=True)
    psutil_process_num_ctx_switches_involuntary = db.Column(
        'psutil_process_num_ctx_switches_involuntary', db.Float, nullable=True)

    __table_args__ = (
        db.PrimaryKeyConstraint('task_id', 'run_id', 'timestamp'),)
