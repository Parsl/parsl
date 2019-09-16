from flask import Flask
from parsl.monitoring.visualization.models import db
import argparse
import os


def cli_run():
    """ Instantiates the Monitoring viz server
    """
    parser = argparse.ArgumentParser(description='Parsl visualization tool')
    parser.add_argument('db_path', type=str, default="sqlite:///{cwd}/monitoring.db".format(cwd=os.getcwd()),
                        nargs="?", help='Database path in the format sqlite:///<absolute_path_to_db>')
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Port at which the monitoring Viz Server is hosted. Default: 8080')
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enable debug logging")
    parser.add_argument("-l", "--listen", type=str, default="127.0.0.1", metavar="ADDRESS",
                        help="Choose address to listen for connections on. Default: 127.0.0.1. Choose 0.0.0.0 to listen on all addresses.")
    args = parser.parse_args()

    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = args.db_path
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    with app.app_context():
        db.create_all()
        from parsl.monitoring.visualization import views
        views.dummy = False
        app.run(host=args.listen, port=args.port, debug=args.debug)


if __name__ == "__main__":
    cli_run()
