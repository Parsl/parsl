from flask import Flask
from viz_server.models import db
import argparse
import os

def cli_run():
    parser = argparse.ArgumentParser(description='Parsl visualization tool')
    parser.add_argument('db_path', type=str,
                        help='Database path')
    parser.add_argument('--port', type=int, default=50550)
    parser.add_argument('--debug', type=str, default='False')

    args = parser.parse_args()
    debug = False
    if args.debug == 'True':
        debug = True
    print("current working dir: {}".format(os.getcwd()))
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = args.db_path
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    with app.app_context():
        db.create_all()
        from viz_server import views
        app.run(host='0.0.0.0', port=args.port, debug=debug)


if __name__ == "__main__":
    cli_run()
