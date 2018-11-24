import pandas as pd
import dash_html_components as html
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.utils import dataframe_to_html_table


sql_conn = get_db()

layout = html.Div(children=[
    html.H1("Workflows"),
    dataframe_to_html_table(id='workflows_table',
                            field='workflow_name',
                            dataframe=pd.read_sql_query("SELECT run_id, "
                                                        "workflow_name, "
                                                        "workflow_version, "
                                                        "time_began, "
                                                        "time_completed, "
                                                        "tasks_completed_count, "
                                                        "tasks_failed_count, "
                                                        "user, "
                                                        "host, "
                                                        "rundir FROM workflows", sql_conn)
                                        .sort_values(
                                            by=['time_began'],
                                            ascending=[False])
                                        .drop_duplicates(
                                            subset=['workflow_name', 'workflow_version'],
                                            keep='last'))
])

close_db()
