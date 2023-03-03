from flask import render_template
from flask import current_app as app
import pandas as pd

import parsl.monitoring.queries.pandas as queries

from parsl.monitoring.visualization.models import Workflow, Task, Status, db

from parsl.monitoring.visualization.plots.default.workflow_plots import task_gantt_plot, task_per_app_plot, workflow_dag_plot
from parsl.monitoring.visualization.plots.default.task_plots import time_series_memory_per_task_plot
from parsl.monitoring.visualization.plots.default.workflow_resource_plots import resource_distribution_plot, resource_efficiency, worker_efficiency

dummy = True

import datetime


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
    for workflow in workflows:
        workflow.status = 'Running'
        if workflow.time_completed is not None:
            workflow.status = 'Completed'
    return render_template('workflows_summary.html', workflows=workflows)


@app.route('/workflow/<workflow_id>/')
def workflow(workflow_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    df_status = queries.status_for_workflow(workflow_id, db.engine)
    df_task = queries.completion_times_for_workflow(workflow_id, db.engine)
    df_task_tries = queries.tries_for_workflow(workflow_id, db.engine)
    task_summary = queries.app_counts_for_workflow(workflow_id, db.engine)

    return render_template('workflow.html',
                           workflow_details=workflow_details,
                           task_summary=task_summary,
                           task_gantt=task_gantt_plot(df_task, df_status, time_completed=workflow_details.time_completed),
                           task_per_app=task_per_app_plot(df_task_tries, df_status, time_completed=workflow_details.time_completed))


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

    task_details = Task.query.filter_by(
        run_id=workflow_id, task_id=task_id).first()
    task_status = Status.query.filter_by(
        run_id=workflow_id, task_id=task_id).order_by(Status.timestamp)

    df_resources = queries.resources_for_task(workflow_id, task_id, db.engine)

    return render_template('task.html',
                           workflow_details=workflow_details,
                           task_details=task_details,
                           task_status=task_status,
                           time_series_memory_resident=time_series_memory_per_task_plot(
                               df_resources, 'psutil_process_memory_resident', 'Memory Usage'),
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
