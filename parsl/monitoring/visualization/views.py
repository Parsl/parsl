import pandas as pd
from flask import current_app as app
from flask import render_template, request
import os.path as ospath

import parsl.monitoring.queries.pandas as queries
from parsl.monitoring.visualization.models import (
    Status,
    Task,
    Workflow,
    db,
    File,
    InputFile,
    OutputFile,
    Environment,
    MiscInfo,
)
from parsl.monitoring.visualization.plots.default.task_plots import (
    time_series_memory_per_task_plot,
)
from parsl.monitoring.visualization.plots.default.workflow_plots import (
    task_gantt_plot,
    task_per_app_plot,
    workflow_dag_plot,
)
from parsl.monitoring.visualization.plots.default.workflow_resource_plots import (
    resource_distribution_plot,
    resource_efficiency,
    worker_efficiency,
)

from parsl.monitoring.visualization.form_fields import FileForm

import datetime

dummy = True


def format_time(value):
    if value is None:
        return value
    elif isinstance(value, float):
        return str(datetime.timedelta(seconds=round(value)))
    elif isinstance(value, datetime.datetime):
        return value.replace(microsecond=0)
    elif isinstance(value, datetime.timedelta):
        rounded_timedelta = datetime.timedelta(days=value.days, seconds=value.seconds)
        return rounded_timedelta
    else:
        return "Incorrect time format found (neither float nor datetime.datetime object)"


def format_duration(value):
    (start, end) = value
    if start and end:
        return format_time(end - start)
    else:
        return "-"


app.jinja_env.filters['timeformat'] = format_time
app.jinja_env.filters['durationformat'] = format_duration


@app.route('/')
def index():
    workflows = Workflow.query.all()
    have_files = []
    for workflow in workflows:
        workflow.status = 'Running'
        if workflow.time_completed is not None:
            workflow.status = 'Completed'
        file_list = File.query.filter_by(run_id=workflow.run_id).first()
        if file_list:
            have_files.append(workflow.run_id)
    return render_template('workflows_summary.html', workflows=workflows, have_files=have_files)


@app.route('/file/<file_id>/')
def file(file_id):
    file_details = File.query.filter_by(file_id=file_id).first()
    input_files = InputFile.query.filter_by(file_id=file_id).all()
    output_files = OutputFile.query.filter_by(file_id=file_id).first()
    task_ids = set()
    environ = None

    for f in input_files:
        task_ids.add(f.task_id)
    if output_files:
        task_ids.add(output_files.task_id)
    tasks = {}
    for tid in task_ids:
        tasks[tid] = Task.query.filter_by(run_id=file_details.run_id, task_id=tid).first()
    workflow_details = Workflow.query.filter_by(run_id=file_details.run_id).first()
    if output_files:
        environ = Environment.query.filter_by(environment_id=tasks[output_files.task_id].task_environment).first()

    return render_template('file_detail.html', file_details=file_details,
                           input_files=input_files, output_files=output_files,
                           tasks=tasks, workflow=workflow_details, environment=environ)


@app.route('/file', methods=['GET', 'POST'])
def file():
    form = FileForm()
    if request.method == 'POST':
        file_list = []
        if form.validate_on_submit():
            if not form.file_name.data.startswith('%'):
                filename = '%' + form.file_name.data
            else:
                filename = form.file_name.data
            file_list = File.query.filter(File.file_name.like(filename)).all()
        return render_template('file.html', form=form, file_list=file_list)
    return render_template('file.html', form=form)


@app.route('/file/workflow/<workflow_id>/')
def file_workflow(workflow_id):
    workflow_files = File.query.filter_by(run_id=workflow_id).all()
    file_map = {}
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()
    task_ids = set()
    files_by_task = {}
    file_details = {}
    for wf in workflow_files:
        file_details[wf.file_id] = wf
        task_ids.add(wf.task_id)
        file_map[wf.file_id] = ospath.basename(wf.file_name)
    tasks = {}

    for tid in task_ids:
        tasks[tid] = Task.query.filter_by(run_id=workflow_id, task_id=tid).first()
        files_by_task[tid] = {'inputs': InputFile.query.filter_by(run_id=workflow_id, task_id=tid).all(),
                              'outputs': OutputFile.query.filter_by(run_id=workflow_id, task_id=tid).all()}
    return render_template('file_workflow.html', workflow=workflow_details,
                           task_files=files_by_task, tasks=tasks, file_map=file_map)


@app.route('/workflow/<workflow_id>/')
def workflow(workflow_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    df_status = queries.status_for_workflow(workflow_id, db.engine)
    df_task = queries.completion_times_for_workflow(workflow_id, db.engine)
    df_task_tries = queries.tries_for_workflow(workflow_id, db.engine)
    task_summary = queries.app_counts_for_workflow(workflow_id, db.engine)
    file_list = File.query.filter_by(run_id=workflow_id).first()
    if file_list:
        have_files = True
    else:
        have_files = False
    misc_info = MiscInfo.query.filter_by(run_id=workflow_id).order_by(MiscInfo.timestamp.asc()).all()
    if misc_info:
        have_misc = True
    else:
        have_misc = False
    return render_template('workflow.html',
                           workflow_details=workflow_details,
                           task_summary=task_summary,
                           task_gantt=task_gantt_plot(df_task, df_status, time_completed=workflow_details.time_completed),
                           task_per_app=task_per_app_plot(df_task_tries, df_status, time_completed=workflow_details.time_completed),
                           have_files=have_files, misc_info=misc_info, have_misc=have_misc)


@app.route('/workflow/<workflow_id>/environment/<environment_id>')
def environment(workflow_id, environment_id):
    environment_details = Environment.query.filter_by(environment_id=environment_id).first()
    workflow = Workflow.query.filter_by(run_id=workflow_id).first()
    task_list = Task.query.filter_by(task_environment=environment_id).all()
    tasks = {}
    for task in task_list:
        if task.task_func_name not in tasks:
            tasks[task.task_func_name] = []
        tasks[task.task_func_name].append(task.task_id)

    return render_template('env.html', environment_details=environment_details,
                           workflow=workflow, tasks=tasks)


@app.route('/workflow/<workflow_id>/app/<app_name>')
def parsl_app(workflow_id, app_name):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    task_summary = Task.query.filter_by(
        run_id=workflow_id, task_func_name=app_name)
    return render_template('app.html',
                           app_name=app_name,
                           workflow_details=workflow_details,
                           task_summary=task_summary)


@app.route('/workflow/<workflow_id>/task/')
def parsl_apps(workflow_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    task_summary = Task.query.filter_by(run_id=workflow_id)
    return render_template('app.html',
                           app_name="All Apps",
                           workflow_details=workflow_details,
                           task_summary=task_summary)


@app.route('/workflow/<workflow_id>/task/<task_id>')
def task(workflow_id, task_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    task_details = queries.full_task_info(workflow_id, task_id, db.engine)
    task_status = Status.query.filter_by(
        run_id=workflow_id, task_id=task_id).order_by(Status.timestamp)

    df_resources = queries.resources_for_task(workflow_id, task_id, db.engine)
    environments = Environment.query.filter_by(run_id=workflow_id).all()
    environs = {}
    for env in environments:
        environs[env.environment_id] = env.label

    return render_template('task.html',
                           workflow_details=workflow_details,
                           task_details=task_details,
                           task_status=task_status,
                           time_series_memory_resident=time_series_memory_per_task_plot(
                               df_resources, 'psutil_process_memory_resident', 'Memory Usage'),
                           environments=environs
                           )


@app.route('/workflow/<workflow_id>/dag_<path>')
def workflow_dag_details(workflow_id, path):
    assert path == "group_by_apps" or path == "group_by_states"

    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()
    query = """SELECT task.task_id, task.task_func_name, task.task_depends, status.task_status_name
               FROM task LEFT JOIN status
               ON task.task_id = status.task_id
               AND task.run_id = status.run_id
               AND status.timestamp = (SELECT MAX(status.timestamp)
                                       FROM status
                                       WHERE status.task_id = task.task_id and status.run_id = task.run_id
                                      )
               WHERE task.run_id='%s'""" % (workflow_id)

    df_tasks = pd.read_sql_query(query, db.engine)

    group_by_apps = (path == "group_by_apps")
    return render_template('dag.html',
                           workflow_details=workflow_details,
                           group_by_apps=group_by_apps,
                           workflow_dag_plot=workflow_dag_plot(df_tasks, group_by_apps=group_by_apps))


@app.route('/workflow/<workflow_id>/resource_usage')
def workflow_resources(workflow_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()
    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    df_resources = queries.resources_for_workflow(workflow_id, db.engine)
    if df_resources.empty:
        return render_template('error.html',
                               message="Workflow %s does not have any resource usage records." % workflow_id)

    df_task = queries.tasks_for_workflow(workflow_id, db.engine)
    df_task_tries = pd.read_sql_query("""SELECT task.task_id, task_func_name,
                                      task_try_time_launched, task_try_time_running, task_try_time_returned from task, try
                                      WHERE task.task_id = try.task_id AND task.run_id='%s' and try.run_id='%s'"""
                                      % (workflow_id, workflow_id), db.engine)
    df_node = queries.nodes_for_workflow(workflow_id, db.engine)

    return render_template('resource_usage.html', workflow_details=workflow_details,
                           user_time_distribution_max_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_time_user', label='CPU Time Distribution', option='max'),
                           memory_usage_distribution_avg_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_memory_resident', label='Memory Distribution', option='avg'),
                           memory_usage_distribution_max_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_memory_resident', label='Memory Distribution', option='max'),
                           cpu_efficiency=resource_efficiency(df_resources, df_node, label='CPU'),
                           memory_efficiency=resource_efficiency(df_resources, df_node, label='mem'),
                           worker_efficiency=worker_efficiency(df_task_tries, df_node),
                           )
