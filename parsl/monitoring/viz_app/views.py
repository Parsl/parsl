from flask import Flask, render_template, request
import pandas as pd
from parsl.monitoring.viz_app.plots.workflow_plots import create_task_gantt
from parsl.monitoring.viz_app.utils import get_db
import requests

app = Flask(__name__)


@app.route('/')
def index():
    db = get_db(app)

    df = pd.read_sql_query("SELECT run_id, workflow_name, workflow_version, time_began, time_completed, "
                           "tasks_completed_count, tasks_failed_count, user, host, rundir FROM workflows", db)\
        .sort_values(by=['time_began'], ascending=[False]).drop_duplicates(subset=['workflow_name', 'workflow_version'], keep='last')

    return render_template('workflows_summary.html', df=df)


@app.route('/workflow/<workflow_id>')
def workflow(workflow_id):
    db = get_db(app)

    workflow_query = db.execute("SELECT workflow_name, user, rundir from workflows WHERE run_id='%s'" % workflow_id)
    workflow_details = workflow_query.fetchone()

    # TODO add logic here to check if there is a workflow with this ID and if there are any tasks to render..

    task_gantt = create_task_gantt(db, workflow_id)

    return render_template('workflow.html', workflow_name=workflow_details[0], workflow_id=workflow_id,
                           owner=workflow_details[1], rundir=workflow_details[2], task_gantt=task_gantt)


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
