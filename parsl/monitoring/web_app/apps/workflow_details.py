import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.plots.default.resource_usage import ResourceUsagePlot
from parsl.monitoring.web_app.plots.default.task_per_app import TaskPerAppPlot
from parsl.monitoring.web_app.plots.default.total_tasks import TotalTasksPlot


layout = html.Div(id='workflow_details')

resource_usage_plot = ResourceUsagePlot('resource_usage_plot_workflow',
                                        setup_args=None,
                                        plot_args=([Input('resource_usage_radio_items', 'value')],
                                                   [State('run_id', 'children')]))

tasks_per_app_plot = TaskPerAppPlot('task_per_app_plot_workflow',
                                    setup_args=None,
                                    plot_args=([Input('run_id', 'children')], []))

total_tasks_plot = TotalTasksPlot('total_tasks_plot_workflow',
                                  setup_args=([Input('run_id', 'children')], []),
                                  plot_args=([Input('bin_width_minutes', 'value'),
                                              Input('bin_width_seconds', 'value')],
                                             [State('run_id', 'children')]))

plots = [resource_usage_plot, tasks_per_app_plot, total_tasks_plot]


@app.callback(Output('workflow_details', 'children'),
              [Input('run_number_dropdown', 'value')])
def workflow_details(run_id):
    return [html.A(id='run_id', children=run_id, hidden=True)] + [plot.html for plot in plots]



# @app.callback(
#     Output('tables', 'children'),
#     [Input('tasks_per_app', 'clickData')],
#     [State('run_id', 'children')])
# def load_task_table(clicked, task_id):
#     print(clicked)
#     if not clicked:
#         return
#     sql_conn = get_db()
#
#     df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(task_id, ))
#     df_task = pd.read_sql_query("SELECT * FROM task WHERE run_id=(?)", sql_conn, params=(task_id, ))
#
#     return [html.Table(
#         [html.Tr([html.Th(col) for col in df_resources.columns])] + \
#         [html.Tr([html.Td(html.A(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].iloc[i][col])) for col in df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].columns]) for i in range(len(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])]))]) for point in clicked['points']]
#