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
