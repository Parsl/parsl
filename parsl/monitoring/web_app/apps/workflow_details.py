import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import timestamp_to_float, timestamp_to_int, num_to_timestamp, DB_DATE_FORMAT


layout = html.Div(id='workflow_details')


@app.callback(Output('workflow_details', 'children'),
              [Input('run_number_dropdown', 'value')])
def workflow_details(run_id):
    return [html.A(id='run_id', children=run_id, hidden=True),
            resource_usage_plot(run_id),
            tasks_per_app_plot(run_id),
            total_tasks_plot(run_id)]


def resource_usage_plot(run_id):
    return html.Div(id='resource_usage_container',
                    children=[dcc.RadioItems(id='resource_usage_radio_items',
                                            options=[{'label': 'User time', 'value': 'psutil_process_time_user'},
                                                     {'label': 'System time', 'value': 'psutil_process_time_system'},
                                                     {'label': 'Memory usage', 'value': 'psutil_process_memory_percent'}],
                                            value='psutil_process_time_user'),
                              dcc.Graph(id='resource_usage_plot_workflow')])


# TODO y_axis labels can't be set with callback from radioitems. Might have to split up the plots
# TODO return html.H3('No resource data found')
@app.callback(Output('resource_usage_plot_workflow', 'figure'),
              [Input('resource_usage_radio_items', 'value')],
              [State('resource_usage_radio_items', 'y_axis'),
               State('run_number_dropdown', 'value')])
def resource_usage_callback(field, y_axis_label, run_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT {field}, timestamp, task_id FROM task_resources WHERE run_id=(?)'.format(field=field),
                                     sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id, ))
    close_db()

    def y_axis_setup():
        dic = dict()
        count = 0
        items = []

        for i in range(len(df_resources)):
            task_id = df_resources.iloc[i]['task_id']
            value = float(df_resources.iloc[i][field])

            if task_id in dic:
                count -= dic[task_id][0]

            dic[task_id] = (value, float(df_task[df_task['task_id'] == task_id]['task_time_returned'].iloc[0]))
            count += value

            remove = []
            for k, v in dic.items():
                if v[1] < timestamp_to_float(df_resources.iloc[i]['timestamp']):
                    count -= v[0]
                    remove.append(k)

            for k in remove: del dic[k]

            items.append(count)

        return items

    return go.Figure(
        data=[go.Scatter(x=df_resources['timestamp'],
                         y=y_axis_setup(),
                         name='tasks')],
        layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                    autorange=True,
                                    title='Time'),
                         yaxis=dict(title=y_axis_label),
                         title='Resource usage')
    )


def tasks_per_app_plot(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id,))
    close_db()

    def y_axis_setup(array):
        count = 0
        items = []
        for n in array:
            if n:
                count += 1
            elif count > 0:
                count -= 1
            items.append(count)

        return items

    # Fill up dict "apps" like: {app1: [#task1, #task2], app2: [#task4], app3: [#task3]}
    apps = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_func_name'] in apps:
            apps[row['task_func_name']].append(row['task_id'])
        else:
            apps[row['task_func_name']] = [row['task_id']]

    return dcc.Graph(id='tasks_per_app_plot',
                     figure=go.Figure(
                         data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                                          y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                                          name=app)
                               for app, tasks in apps.items()] +
                              [go.Scatter(x=df_status['timestamp'],
                                          y=y_axis_setup(df_status['task_status_name'] == 'running'),
                                          name='all')],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(tickformat= ',d',
                                                     title='Tasks'),
                                          hovermode='closest',
                                          title='Tasks per app')
                     ))


def total_tasks_plot(run_id, columns=20):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))

    time_step = int((max_time - min_time) / columns)
    minutes = time_step // 60
    seconds = time_step % 60

    return html.Div(id='total_tasks_container',
                    children=[html.P('Bin width'),
                              html.Label(htmlFor='bin_width_minutes', children='Minutes'),
                              dcc.Input(id='bin_width_minutes', type='number', min=0, value=minutes),
                              html.Label(htmlFor='bin_width_seconds', children='Seconds'),
                              dcc.Input(id='bin_width_seconds', type='number', min=0, value=seconds),
                              dcc.Graph(id='total_tasks_plot_workflow')])


@app.callback(Output('total_tasks_plot_workflow', 'figure'),
              [Input('bin_width_minutes', 'value'),
               Input('bin_width_seconds', 'value')],
              [State('run_number_dropdown', 'value')])
def total_tasks_callback(minutes, seconds, run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))

    time_step = 60 * minutes + seconds

    x_axis = []
    for i in range(min_time, max_time, time_step):
        x_axis.append(num_to_timestamp(i + time_step / 2).strftime(DB_DATE_FORMAT))

    def y_axis_setup(value):
        items = []
        for i in range(len(x_axis) - 1):
            x = df_status['timestamp'] >= x_axis[i]
            y = df_status['timestamp'] < x_axis[i+1]
            items.append(sum(df_status.loc[[a and b for a, b in zip(x, y)]]['task_status_name'] == value))

        return items

    return go.Figure(data=[go.Bar(x=x_axis[:-1],
                                  y=y_axis_setup('done'),
                                  name='done'),
                           go.Bar(x=x_axis[:-1],
                                  y=y_axis_setup('failed'),
                                  name='failed')],
                     layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time'),
                                      yaxis=dict(tickformat= ',d',
                                                 title='Tasks / ' + num_to_timestamp(max_time - min_time).strftime('%Mm%Ss')),
                                      barmode='stack',
                                      title='Total tasks')
                     )








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