from typing import Any

import pandas as pd

# pandas can take several different types of database connection,
# and itself exposes its connection parameters as "Any".
# This alias might help things be a bit clearer in our documentation.
DB = Any


def app_counts_for_workflow(workflow_id: Any, db: DB) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT task_func_name, count(*) as 'frequency'
          FROM task
         WHERE run_id='%s'
      GROUP BY task_func_name;
         """ % workflow_id, db)


def nodes_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT *
          FROM node
         WHERE run_id='%s';
        """ % (workflow_id), db)


def resources_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT *
          FROM resource
         WHERE run_id='%s';
        """ % (workflow_id), db)


def resources_for_task(workflow_id: Any, task_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT *
          FROM resource
         WHERE run_id='%s' AND task_id='%s';
        """ % (workflow_id, task_id), db)


def status_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT run_id, task_id, task_status_name, timestamp
          FROM status
         WHERE run_id='%s';
        """ % workflow_id, db)


def completion_times_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT task_id, task_func_name, task_time_returned
          FROM task
         WHERE run_id='%s'
        """ % (workflow_id), db)


def tasks_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT *
          FROM task
         WHERE run_id='%s'
        """ % (workflow_id), db)


def tries_for_workflow(workflow_id: Any, db: Any) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT task.task_id, task_func_name, task_time_returned, task_try_time_running, task_try_time_returned
          FROM task, try
         WHERE task.task_id = try.task_id AND task.run_id='%s' AND try.run_id='%s'
        """ % (workflow_id, workflow_id), db)
