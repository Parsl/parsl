import pandas as pd
import plotly.figure_factory as ff
from plotly.offline import plot


def create_task_gantt(db, workflow_id):

    # TODO: Need error checking here, e.g., if timestamps arent yet known etc.

    df_task = pd.read_sql_query('SELECT task_id, task_time_submitted, task_time_returned from task WHERE run_id=(?)',
                                db, params=(workflow_id,))
    df_task = df_task.sort_values(by=['task_time_submitted'], ascending=False)

    df_task['task_time_submitted'] = pd.to_datetime(df_task['task_time_submitted'], unit='s')
    df_task['task_time_returned'] = pd.to_datetime(df_task['task_time_returned'], unit='s')

    df_task = df_task.rename(index=str, columns={"task_id": "Task",
                                                 "task_time_submitted": "Start",
                                                 "task_time_returned": "Finish"})

    parsl_tasks = df_task.to_dict('records')

    fig = ff.create_gantt(parsl_tasks)
    div = plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
    return div
