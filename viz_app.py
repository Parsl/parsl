#!flask/bin/python
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////home/kyle/parsl/parsl/parsl/tests/manual_tests/monitoring.db'
db = SQLAlchemy(app)
