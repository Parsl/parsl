import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, init_db
import argparse


def web_app(db, port):
    if not init_db(db):
        return

    print(' * Running on http://localhost:' + str(port))

    from parsl.monitoring.web_app.apps import sql, workflows, workflow_details

    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.Nav(children=[
            dcc.Link('Workflows', href='/workflows'),
            dcc.Link('SQL', href='/sql')]),
        html.Div(id='page-content')
    ])

    @app.callback(Output('page-content', 'children'),
                  [Input('url', 'pathname')])
    def display_page(pathname):
        if pathname == '/': # TODO Redirect to workflows or show special page
            pass
        elif pathname == '/workflows':
            return workflows.layout
        elif '/workflows' in str(pathname):
            return workflow_details.display_workflow(run_id=pathname.split('/').pop())
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

    print('db =', db)
    print('Visualization port =', port)

    web_app(db, port)
