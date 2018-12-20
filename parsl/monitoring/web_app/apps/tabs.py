import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.apps import workflow_details, apps_details, tasks_details


def display_workflow(run_id):
    sql_conn = get_db()
    df_workflow = pd.read_sql_query('SELECT workflow_name, time_began, rundir, run_id FROM workflows WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))

    df_workflows = pd.read_sql_query('SELECT workflow_name, time_began, rundir, run_id FROM workflows WHERE workflow_name=(?)',
                                     sql_conn, params=(df_workflow['workflow_name'][0], ))
    close_db()

    return html.Div(children=[
        html.H2('Workflow name'),
        html.H4(id='workflow_name', children=df_workflows['workflow_name'][0]),
        html.H2('Run Id'),
        html.H4(id='run_id', children=run_id),
        html.H2('Version'),
        html.H4(id='run_version', children=df_workflow['rundir'].iloc[0].split('/').pop()),
        dropdown(dataframe=df_workflows.sort_values(by='time_began', ascending=False)),
        dcc.Tabs(id="tabs", value='workflow', children=[
            dcc.Tab(label='Workflow', value='workflow'),
            dcc.Tab(label='Apps', value='apps'),
            dcc.Tab(label='Tasks', value='tasks'),
        ]),
        html.Div(id='tabs_content')
    ])


@app.callback(Output('tabs_content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'workflow':
        return workflow_details.layout
    elif tab == 'apps':
        return apps_details.layout
    elif tab == 'tasks':
        return tasks_details.layout


def dropdown(dataframe):
    options = []

    latest = dataframe['run_id'].iloc[0]
    options.append(dcc.Link(dataframe['rundir'].iloc[0].split('/').pop() + ' (Latest)', href='/workflows/' + latest))

    for i in range(1, len(dataframe)):
        run_id = dataframe['run_id'].iloc[i]
        options.append(dcc.Link(dataframe['rundir'].iloc[i].split('/').pop(), href='/workflows/' + run_id))

    return html.Div(
        className='dropdown',
        children=[
            html.Button(
                className='dropbtn',
                children='Select version'
            ),
            html.Div(
                className='dropdown-content',
                children=options
            )]
    )
