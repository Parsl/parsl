from viz_app import app, db
from models import *
from views import *


def create_db():
    db.create_all()

if __name__ == '__main__':
    create_db()
    app.run(port=5555, debug=True)
