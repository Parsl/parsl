import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.plots.default.task_per_app import TaskPerAppMultiplePlot
from parsl.monitoring.web_app.plots.default.total_tasks import TotalTasksMultiplePlot

layout = html.Div(id='tasks_details')

tasks_per_app_multiple_plot = TaskPerAppMultiplePlot('task_per_app_multiple_plot_tasks',
                                                     plot_args=([Input('apps_dropdown', 'value')],
                                                                [State('run_id', 'children')]))

total_tasks_multiple_plot = TotalTasksMultiplePlot('total_tasks_multiple_plot_tasks',
                                                   plot_args=([Input('bin_width_minutes', 'value'),
                                                               Input('bin_width_seconds', 'value'),
                                                               Input('apps_dropdown', 'value')],
                                                              [State('run_id', 'children')]))

plots = [tasks_per_app_multiple_plot, total_tasks_multiple_plot]


@app.callback(Output('tasks_details', 'children'),
              [Input('run_id', 'children')])
def tasks_details(run_id):
    sql_conn = get_db()
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id,))
    close_db()

    apps = []
    for _app in df_task['task_func_name'].unique():
        apps.append(dict(label=_app, value=_app))

    return [dcc.Dropdown(
        id='apps_dropdown',
        options=apps,
        value=apps[0],
        multi=True)] + [plot.html(run_id) for plot in plots]

