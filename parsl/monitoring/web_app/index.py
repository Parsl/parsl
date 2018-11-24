import sys
import argparse


try:
    import sqlite3
    import pandas
    import dash_core_components as dcc
    import dash_html_components as html
    from dash.dependencies import Input, Output
except ImportError:
    viz_tool_enabled = False
else:
    sql3 = sqlite3
    pd = pandas
    viz_tool_enabled = True


def web_app(db, port):
    if not viz_tool_enabled:
        print("Missing modules for the optional feature monitoring. Please run pip install parsl[monitoring]", file=sys.stderr)
        return

    from parsl.monitoring.web_app.app import app, init_db

    # TODO Find out if db is created after script finalizes or during its execution. If the latter, this is a race condition that needs a fix
    if not init_db(db):
        print("Visualization tool failed to initialize. " + db + " hasn't been created yet", file=sys.stderr)
        return

    print(' * Visualizing ' + db.split('/').pop())
    print(' * Running on http://localhost:' + str(port) + '/workflows')

    from parsl.monitoring.web_app.apps import sql, workflows, tabs

    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.Div(id='page-content')
    ])

    @app.callback(Output('page-content', 'children'),
                  [Input('url', 'pathname')])
    def display_page(pathname):
        if pathname == '/':  # TODO Redirect to workflows or show special page
            pass
        elif pathname == '/workflows':
            return workflows.layout
        elif '/workflows' in str(pathname):
            return tabs.display_workflow(workflow_name=pathname.split('/').pop())
        elif pathname == '/sql':
            return sql.layout
        else:
            return '404'

    # Setting use_reloader to false prevents the script to be ran twice
    app.run_server(port=port, debug=True, use_reloader=False)


# TODO Automatically searching for .db files would be a nice touch
def cli_run():
    parser = argparse.ArgumentParser(description='Parsl visualization tool')
    parser.add_argument('db', type=str,
                        help='Database file')
    parser.add_argument('--db_dir', type=str, metavar='DIR', default='./',
                        help='Database location')
    parser.add_argument('--port', type=int, default=8050)

    args = parser.parse_args()

    web_app(args.db_dir + args.db, args.port)


def run(monitoring_config):
    db = monitoring_config.eng_link.split('/').pop()
    port = monitoring_config.web_app_port + 1

    web_app(db, port)
