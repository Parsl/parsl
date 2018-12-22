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

    if df_workflow.empty:
        close_db()
        return 'Run id ' + run_id + ' not found'

    df_workflows = pd.read_sql_query('SELECT workflow_name, time_began, rundir, run_id FROM workflows WHERE workflow_name=(?)',
                                     sql_conn, params=(df_workflow['workflow_name'][0], ))
    close_db()

    return html.Div(children=[
        html.Div(className='flex_container',
                 children=[
                     html.Img(className='parsl_logo',
                              src='../../../assets/parsl-logo.png'),
                     html.Div(className='title_container',
                              children=[
                                  html.H3('Workflow name'),
                                  html.P(id='workflow_name', children=df_workflows['workflow_name'][0]),
                              ]),
                     html.Div(className='title_container',
                              children=[
                                  html.H3('Run Id'),
                                  html.P(id='run_id', children=run_id),
                              ]),
                     html.Div(className='title_container',
                              children=[
                                  html.H3('Version'),
                                  html.P(id='run_version', children=df_workflow['rundir'].iloc[0].split('/').pop())
                              ]),

                     html.Div(className='dropdown_container',
                              children=[
                                  version_dropdown(dataframe=df_workflows.sort_values(by='time_began', ascending=False)),
                                  view_dropdown(run_id)
                              ]
                     )
                 ]),
        html.Div(id='nav_content')
    ])


@app.callback(Output('nav_content', 'children'),
              [Input('url', 'pathname')])
def render_content(pathname):
    if '/workflow-view' in str(pathname):
        return workflow_details.layout
    elif '/apps-view' in str(pathname):
        return apps_details.layout
    elif '/tasks-view' in str(pathname):
        return tasks_details.layout
    else:
        return html.H2('Select a view from the menu above', style=dict(textAlign='center'))


def view_dropdown(run_id):
    return html.Div(
        className='dropdown',
        children=[
            html.Button(
                className='dropbtn',
                children=[
                    'View',
                    html.Img(className='',
                             src='../../../assets/caret-down.png'),
                ]
            ),

            html.Div(
                className='dropdown-content',
                children=[
                    dcc.Link('Workflow', href='/workflows/' + run_id + '/workflow-view'),
                    dcc.Link('Apps', href='/workflows/' + run_id + '/apps-view'),
                    dcc.Link('Tasks', href='/workflows/' + run_id + '/tasks-view/0')
                ]
            )]
    )


def version_dropdown(dataframe):
    options = []

    latest = dataframe['run_id'].iloc[0]
    options.append(dcc.Link(dataframe['rundir'].iloc[0].split('/').pop() + ' (Latest)', href='/workflows/' + latest + '/workflow-view'))

    for i in range(1, len(dataframe)):
        run_id = dataframe['run_id'].iloc[i]
        options.append(dcc.Link(dataframe['rundir'].iloc[i].split('/').pop(), href='/workflows/' + run_id + '/workflow-view'))

    return html.Div(
        className='dropdown',
        children=[
            html.Button(
                className='dropbtn',
                children=[
                    'Version',
                    html.Img(className='',
                             src='../../../assets/caret-down.png'),
                ]
            ),

            html.Div(
                className='dropdown-content',
                children=options
            )]
    )
