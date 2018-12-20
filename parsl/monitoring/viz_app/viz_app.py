import argparse
import sys


def viz_app(db, port):
    try:
        import sqlite3  # noqa # pylint: disable=unused-import
        import pandas  # noqa # pylint: disable=unused-import
    except ImportError:
        print("Missing modules for the optional feature monitoring. Please run pip install parsl[visualization]", file=sys.stderr)
        return

    from parsl.monitoring.viz_app.views import app
    from parsl.monitoring.viz_app.utils import init_db

    # TODO Find out if db is created after script finalizes or during its execution.
    #  If the latter, this is a race condition that needs a fix
    if not init_db(app, db):
        print("Visualization tool failed to initialize. " + db + " hasn't been created yet", file=sys.stderr)
        return

    print(' * Visualizing ' + db.split('/').pop())
    print(' * Running on http://localhost:' + str(port) + '/')

    app.run(port=port, debug=True, use_reloader=False)


def cli_run():
    parser = argparse.ArgumentParser(description='Parsl visualization tool')
    parser.add_argument('db_path', type=str, help='Database path')
    parser.add_argument('--port', type=int, default=8050)
    args = parser.parse_args()
    viz_app(args.db_path, args.port)


def run(monitoring_config):
    db = monitoring_config.store.connection_string.split('/').pop()
    port = monitoring_config.visualization_server.port

    viz_app(db, port)


if __name__ == '__main__':
    viz_app(sys.argv[1], 5555)
