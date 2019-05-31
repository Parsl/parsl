from flask import render_template
from flask import current_app as app
import pandas as pd
from parsl.monitoring.visualization.models import *

from parsl.monitoring.visualization.plots.default.workflow_plots import task_gantt_plot, task_per_app_plot, workflow_dag_plot
from parsl.monitoring.visualization.plots.default.task_plots import time_series_cpu_per_task_plot, time_series_memory_per_task_plot
from parsl.monitoring.visualization.plots.default.workflow_resource_plots import resource_distribution_plot, resource_time_series

dummy = True


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

#     df_task = pd.read_sql_query("""SELECT task.task_id, task.task_func_name, task.task_time_submitted,
#                                 status.timestamp, task.task_time_returned from task, status
#                                 WHERE task.run_id='%s' and status.run_id=task.run_id and
#                                 status.task_status_name='running' and status.task_id=task.task_id"""
#                                 % (workflow_id), db.engine)
    df_status = pd.read_sql_query(
        "SELECT run_id, task_id, task_status_name, timestamp FROM status WHERE run_id='%s'" % workflow_id, db.engine)
    df_task = pd.read_sql_query("""SELECT task_id, task_func_name, task_time_submitted,
                                task_time_returned, task_time_running from task
                                WHERE run_id='%s'"""
                                % (workflow_id), db.engine)
    task_summary = db.engine.execute(
        "SELECT task_func_name, count(*) as 'frequency' from task WHERE run_id='%s' group by task_func_name;" % workflow_id)

    return render_template('workflow.html',
                           workflow_details=workflow_details,
                           task_summary=task_summary,
                           task_gantt=task_gantt_plot(df_task),
                           task_per_app=task_per_app_plot(df_task, df_status))


@app.route('/workflow/<workflow_id>/app/')
def parsl_apps(workflow_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    task_summary = Task.query.filter_by(run_id=workflow_id)
    return render_template('app.html',
                           app_name="All Apps",
                           workflow_details=workflow_details,
                           task_summary=task_summary)


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


@app.route('/workflow/<workflow_id>/task/<task_id>')
def task(workflow_id, task_id):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()

    if workflow_details is None:
        return render_template('error.html', message="Workflow %s could not be found" % workflow_id)

    task_details = Task.query.filter_by(
        run_id=workflow_id, task_id=task_id).first()
    task_status = Status.query.filter_by(
        run_id=workflow_id, task_id=task_id).order_by(Status.timestamp)

    df_resources = pd.read_sql_query(
        "SELECT * FROM resource WHERE run_id='%s' AND task_id='%s'" % (workflow_id, task_id), db.engine)

    return render_template('task.html',
                           workflow_details=workflow_details,
                           task_details=task_details,
                           task_status=task_status,
                           # time_series_cpu_time=time_series_cpu_per_task_plot(df_resources, 'psutil_process_time_user', 'CPU user time'),
                           time_series_cpu_percent=time_series_cpu_per_task_plot(
                               df_resources, 'psutil_process_cpu_percent', 'CPU Utilization'),
                           time_series_memory_resident=time_series_memory_per_task_plot(
                               df_resources, 'psutil_process_memory_resident', 'Memory Usage'),
                           # time_series_memory_percent=time_series_memory_per_task_plot(df_resources, 'psutil_process_memory_percent', 'Memory utilization')
                           )


@app.route('/workflow/<workflow_id>/dag_<path:path>')
@app.route('/workflow/<workflow_id>/dag_<path:path>')
def workflow_dag_details(workflow_id, path='group_by_apps'):
    workflow_details = Workflow.query.filter_by(run_id=workflow_id).first()
    df_tasks = pd.read_sql_query("""SELECT task_id, task_func_name, task_depends,
                                 task_time_submitted, task_time_returned, task_time_running
                                 FROM task WHERE run_id='%s' """
                                 % (workflow_id), db.engine)

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

    df_resources = pd.read_sql_query(
        "SELECT * FROM resource WHERE run_id='%s'" % (workflow_id), db.engine)
    if df_resources.empty:
        return render_template('error.html',
                               message="Workflow %s does not have any resource usage records." % workflow_id)

    df_task = pd.read_sql_query(
        "SELECT * FROM task WHERE run_id='%s'" % (workflow_id), db.engine)

    df_task_resources = pd.read_sql_query('''
                                          SELECT task_id, timestamp, resource_monitoring_interval,
                                          psutil_process_cpu_percent, psutil_process_time_user,
                                          psutil_process_memory_percent, psutil_process_memory_resident
                                          from resource
                                          where run_id = '%s'
                                          ''' % (workflow_id), db.engine)

    return render_template('resource_usage.html', workflow_details=workflow_details,
                           user_time_distribution_avg_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_time_user', label='CPU Time Distribution', option='avg'),
                           user_time_distribution_max_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_time_user', label='CPU Time Distribution', option='max'),
                           memory_usage_distribution_avg_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_memory_resident', label='Memory Distribution', option='avg'),
                           memory_usage_distribution_max_plot=resource_distribution_plot(
                               df_resources, df_task, type='psutil_process_memory_resident', label='Memory Distribution', option='max'),
                           user_time_time_series=resource_time_series(
                               df_task_resources, type='psutil_process_time_user', label='CPU User Time'),
                           cpu_percent_time_series=resource_time_series(
                               df_task_resources, type='psutil_process_cpu_percent', label='CPU Utilization'),
                           memory_percent_time_series=resource_time_series(
                               df_task_resources, type='psutil_process_memory_percent', label='Memory Utilization'),
                           memory_resident_time_series=resource_time_series(
                               df_task_resources, type='psutil_process_memory_resident', label='Memory Usage'),
                           )
