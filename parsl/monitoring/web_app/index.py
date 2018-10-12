import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, init_db
import sys
import getopt


def web_app(db, port):
    if not init_db(db):
        return

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
    argv = sys.argv[1:]
    db_name = 'parsl.db'
    db_dir = './'
    port = 8050
    try:
        opts, args = getopt.getopt(argv,'h',['db_dir=','db_name=','port='])
    except getopt.GetoptError:
        print('Invalid argument')
        print('parsl-visualize --db_dir <db_dir> --db_name <db_name> --port <port>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('parsl-visualize --db_dir <db_dir> --db_name <db_name> --port <port>')
            sys.exit()
        elif opt in ('--db_dir', ):
            db_dir = arg
        elif opt in ('--db_name', ):
            db_name = arg
        elif opt in ('--port', ):
            if not arg.isdigit():
                print('Port number must be an integer')
                sys.exit(2)
            port = arg

    print('db_name =', db_name)
    print('db_dir =', db_dir)
    print('port =', port)

    web_app(db_dir + db_name, port)


def run(monitoring_config):
    db = monitoring_config.eng_link.split('/').pop()
    port = monitoring_config.web_app_port + 1

    print('db =', db)
    print('Visualization port =', port)

    web_app(db, port)


if __name__ == '__main__':
    cli_run()
