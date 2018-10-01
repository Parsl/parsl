import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.utils import generate_table1

layout = html.Div(children=[
    html.H1("Tasks"),
    generate_table1("SELECT * FROM task"),
    html.H1("Task Resources"),
    generate_table1("SELECT * FROM task_resources"),
    html.H1("Task Status"),
    generate_table1("SELECT * FROM task_status"),
    html.H1("Workflows"),
    generate_table1("SELECT * FROM workflows")
])