import os
import sqlite3
from flask import g
from flask import Flask, render_template, request
import pandas as pd
import requests

app = Flask(__name__)


@app.route('/')
def index():
    db = get_db()

    df = pd.read_sql_query("SELECT run_id, workflow_name, workflow_version, time_began, time_completed, "
                           "tasks_completed_count, tasks_failed_count, user, host, rundir FROM workflows", db)\
        .sort_values(by=['time_began'], ascending=[False]).drop_duplicates(subset=['workflow_name', 'workflow_version'], keep='last')

    return render_template('workflows_summary.html', df=df)


@app.route('/workflow/<workflow_id>/')
def workflow(workflow_id):
    db = get_db()

    workflow_query = db.execute("SELECT workflow_name, user, rundir from workflows WHERE run_id='%s'" % workflow_id)
    workflow_details = workflow_query.fetchone()

# TODO add logic here to check if there is a workflow with this ID and if there are any tasks to render..

    from parsl.monitoring.viz_app.plots.default.task_gantt import task_gantt_plot
    from parsl.monitoring.viz_app.plots.default.memory_usage import \
        memory_usage_distribution_plot, \
        resident_memory_usage_distribution_plot, \
        virtual_memory_usage_distribution_plot
    from parsl.monitoring.viz_app.plots.default.system_time import system_time_distribution_plot
    from parsl.monitoring.viz_app.plots.default.user_time import user_time_distribution_plot
    from parsl.monitoring.viz_app.plots.default.total_tasks import total_tasks_plot
    from parsl.monitoring.viz_app.plots.default.task_per_app import task_per_app_plot

    return render_template('workflow.html', workflow_name=workflow_details[0], workflow_id=workflow_id,
                           owner=workflow_details[1], rundir=workflow_details[2],
                           task_gantt=task_gantt_plot(workflow_id),
                           task_per_app=task_per_app_plot(workflow_id),
                           total_tasks=total_tasks_plot(workflow_id),
                           user_time_distribution_avg_plot=user_time_distribution_plot(workflow_id, 'avg'),
                           user_time_distribution_max_plot=user_time_distribution_plot(workflow_id, 'max'),
                           system_time_distribution_avg_plot=system_time_distribution_plot(workflow_id, 'avg'),
                           system_time_distribution_max_plot=system_time_distribution_plot(workflow_id, 'max'),
                           memory_usage_distribution_avg_plot=memory_usage_distribution_plot(workflow_id, 'avg'),
                           memory_usage_distribution_max_plot=memory_usage_distribution_plot(workflow_id, 'max'),
                           resident_memory_usage_distribution_avg_plot=resident_memory_usage_distribution_plot(workflow_id, 'avg'),
                           resident_memory_usage_distribution_max_plot=resident_memory_usage_distribution_plot(workflow_id, 'max'),
                           virtual_memory_usage_distribution_avg_plot=virtual_memory_usage_distribution_plot(workflow_id, 'avg'),
                           virtual_memory_usage_distribution_max_plot=virtual_memory_usage_distribution_plot(workflow_id, 'max'),)


@app.route('/workflow/<workflow_id>/task/<task_id>')
def task(workflow_id, task_id):
    db = get_db()

    workflow_query = db.execute("SELECT workflow_name, user, rundir from workflows WHERE run_id='%s'" % workflow_id)
    workflow_details = workflow_query.fetchone()

    tasks = db.execute("SELECT task_id from task WHERE run_id='%s'" % workflow_id)
    tasks = tasks.fetchall()


    from parsl.monitoring.viz_app.plots.default.task_time_series import \
        time_series_user_time_per_task_plot, \
        time_series_system_time_per_task_plot, \
        time_series_virtual_memory_usage_per_task_plot, \
        time_series_resident_memory_usage_per_task_plot

    return render_template('task.html', workflow_name=workflow_details[0], workflow_id=workflow_id,
                           owner=workflow_details[1], rundir=workflow_details[2], tasks=tasks, task_id=task_id,
                           time_series_user_time=time_series_user_time_per_task_plot(workflow_id, task_id),
                           time_series_system_time=time_series_system_time_per_task_plot(workflow_id, task_id),
                           time_series_virtual_memory_usage=time_series_virtual_memory_usage_per_task_plot(workflow_id, task_id),
                           time_series_resident_memory_usage=time_series_resident_memory_usage_per_task_plot(workflow_id, task_id))


@app.route('/shutdown', methods=['POST'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'


def shutdown_web_app(host, port):
    print(host + ':' + str(port) + '/shutdown')
    print(requests.post(host + ':' + str(port) + '/shutdown', data=''))


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def init_db(db):
    if os.path.isfile(db):
        app.config.update(dict(DATABASE=db))
        return True
    else:
        return False


def get_db():
    with app.app_context():
        if 'db' not in g:
            g.db = sqlite3.connect(
                app.config['DATABASE'],
                detect_types=sqlite3.PARSE_DECLTYPES
            )

            g.db.row_factory = sqlite3.Row

        return g.db


def close_db():
    with app.app_context():
        db = g.pop('db', None)

        if db is not None:
            db.close()
