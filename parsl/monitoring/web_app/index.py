import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, init_db
import sys, getopt


def web_app(db, port):
    if not init_db(db):
        return

    from parsl.monitoring.web_app.apps import sql, workflows

    app.layout = html.Div([
        html.Link(href='/assets/styles.css', rel='stylesheet'),
        dcc.Location(id='url', refresh=False),
        html.Nav(children=[
            dcc.Link('Workflows', href='/workflows'),
            dcc.Link('SQL', href='/sql')]),
        html.Div(id='page-content')
    ])

    @app.callback(Output('page-content', 'children'),
                  [Input('url', 'pathname')])
    def display_page(pathname):
        if pathname == "/":
            dcc.Location(id='url', refresh=True, pathname="/workflows"),
        elif pathname == "/workflows":
            return workflows.layout
        elif pathname == "/sql":
            return sql.layout
        else:
            return '404'

    app.run_server(port=port, debug=True, use_reloader=False)


# TODO CSS is not being imported when ran through cli
def cli_run(argv):
    # argv = sys.argv[1:]
    db_name = 'parsl.db'
    db_dir = './'
    port = 8050
    try:
        opts, args = getopt.getopt(argv,"hdb_dir:db_name:port:",["db_dir=","db_name=","port="])
    except getopt.GetoptError:
        print('parsl-visualize --db_dir <db_dir> --db_name <db_name> --port <port>')
        sys.exit(2)
    for opt, arg in opts:
        print(opts)
        if opt == '-h':
            print('parsl-visualize --db_dir <db_dir> --db_name <db_name> --port <port>')
            sys.exit()
        elif opt in ("--db_dir", ):
            db_dir = arg
        elif opt in ("--db_name", ):
            db_name = arg
        elif opt in ("--port", ):
            port = arg

    print('DB name is', db_name)
    print('DB dir is', db_dir)
    print('Port is', port)

    web_app(db_dir + db_name, port)


def run(monitoring_config):
    db = monitoring_config.eng_link.split('/').pop()
    port = monitoring_config.web_app_port + 1
    web_app(db, port)


if __name__ == '__main__':
    cli_run(argv = sys.argv[1:])