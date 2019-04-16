import re
import sys
import argparse

from parsl.monitoring.web_app.utils import RUN_ID_REGEX


def web_app(db, port):
    try:
        import sqlite3  # noqa # pylint: disable=unused-import
        import pandas  # noqa # pylint: disable=unused-import
        import dash_core_components as dcc
        import dash_html_components as html
        from dash.dependencies import Input, Output
    except ImportError:
        print("Missing modules for the optional feature monitoring. Please run pip install parsl[monitoring]", file=sys.stderr)
        return

    from parsl.monitoring.web_app.app import app, init_db

    # TODO Find out if db is created after script finalizes or during its execution.
    #  If the latter, this is a race condition that needs a fix

    if not init_db(db):
        print("Visualization tool failed to initialize. " + db + " hasn't been created yet", file=sys.stderr)
        return

    print(' * Visualizing ' + db.split('/').pop())
    print(' * Running on http://localhost:' + str(port) + '/workflows')

    from parsl.monitoring.web_app.apps import workflows, nav_bar

    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.Div(id='page_content')
    ])

    @app.callback(Output('page_content', 'children'),
                  [Input('url', 'pathname')])
    def display_page(pathname):
        if pathname == '/':  # TODO Redirect to workflows or show special page
            pass
        elif pathname == '/workflows':
            return workflows.layout
        elif '/workflows' in str(pathname):
            pathname_split = pathname.split('/')
            run_id = pathname_split[pathname_split.index('workflows') + 1]
            if run_id and re.fullmatch(RUN_ID_REGEX, run_id):
                return nav_bar.display_workflow(run_id=run_id)
            else:
                return'Invalid run id'
        else:
            return '404'

    # Setting use_reloader to false prevents the script to be ran twice
    app.run_server(port=port, debug=True, use_reloader=False)


# TODO Automatically searching for .db files would be a nice touch
def cli_run():
    parser = argparse.ArgumentParser(description='Parsl visualization tool')
    parser.add_argument('db_path', type=str,
                        help='Database path')
    parser.add_argument('--port', type=int, default=8050)

    args = parser.parse_args()

    web_app(args.db_path, args.port)


def run(monitoring_config):
    db = monitoring_config.store.connection_string.split('/').pop()
    port = monitoring_config.visualization_server.port

    web_app(db, port)
