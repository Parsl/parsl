# reruns a monitoring.db file into a list-of-log-objects structure
# ready for my usual observability techniques

import sqlite3
from datetime import datetime


def import_db(filename: str) -> list[dict]:
    """Turn the supplied monitoring.db into a sequence of events."""
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    results = cursor.execute("SELECT task_id, try_id, run_id, task_status_name, timestamp FROM status")

    events = []
    while (r := results.fetchone()) is not None:
        (task_id, try_id, run_id, task_status_name, timestamp) = r
        d = {"parsl_dfk": run_id,
             "parsl_task_id": task_id,
             "parsl_try_id": try_id,
             "parsl_task_status": task_status_name,
             "created": datetime.fromisoformat(timestamp).timestamp()
             }
        events.append(d)

    results = cursor.execute("SELECT task_id, run_id, task_func_name FROM task")

    while (r := results.fetchone()) is not None:
        (task_id, run_id, task_func_name) = r
        d = {"parsl_dfk": run_id,
             "parsl_task_id": task_id,
             "parsl_app_name": task_func_name,
             # note no created!
             }
        events.append(d)

    results = cursor.execute("SELECT run_id, workflow_name, workflow_version, time_began, "
                             "time_completed, host, user, rundir, tasks_failed_count, tasks_completed_count "
                             "FROM workflow")

    while (r := results.fetchone()) is not None:
        (run_id, workflow_name, workflow_version, time_began, time_completed, host, user, rundir, tasks_failed_count, tasks_completed_count) = r
        for t in (time_began, time_completed):
            if t is None:
                continue  # might not be a completed time...
            d = {"parsl_dfk": run_id,
                 "parsl_workflow_name": workflow_name,
                 "parsl_workflow_version": workflow_version,
                 "created": datetime.fromisoformat(t).timestamp(),
                 "parsl_workflow_host": host,
                 "parsl_workflow_user": user,
                 "parsl_rundir": rundir,
                 "parsl_workflow_tasks_failed_count": tasks_failed_count,
                 "parsl_workflow_tasks_completed_count": tasks_completed_count
                 }
            events.append(d)

    results = cursor.execute("SELECT try_id, task_id, run_id, timestamp, "
                             "resource_monitoring_interval, psutil_process_pid, "
                             "psutil_process_memory_percent, "
                             "psutil_process_children_count, "
                             "psutil_process_time_user, "
                             "psutil_process_time_system, "
                             "psutil_process_memory_virtual, "
                             "psutil_process_memory_resident, "
                             "psutil_process_disk_read, "
                             "psutil_process_disk_write, "
                             "psutil_process_status, "
                             "psutil_cpu_num, "
                             "psutil_process_num_ctx_switches_voluntary, "
                             "psutil_process_num_ctx_switches_involuntary from RESOURCE")

    while (r := results.fetchone()) is not None:

        (try_id, task_id, run_id, timestamp, resource_monitoring_interval,
         psutil_process_pid, psutil_process_memory_percent,
         psutil_process_children_count, psutil_process_time_user,
         psutil_process_time_system, psutil_process_memory_virtual,
         psutil_process_memory_resident, psutil_process_disk_read,
         psutil_process_disk_write, psutil_process_status, psutil_cpu_num,
         psutil_process_num_ctx_switches_voluntary, psutil_process_num_ctx_switches_involuntary) = r

        d = {"parsl_try_id": try_id,
             "parsl_task_id": task_id,
             "parsl_dfk": run_id,
             "created": datetime.fromisoformat(timestamp).timestamp(),
             "resource_monitoring_interval": resource_monitoring_interval,
             "psutil_process_pid": psutil_process_pid,
             "psutil_process_memory_percent": psutil_process_memory_percent,
             "psutil_process_children_count": psutil_process_children_count,
             "psutil_process_time_user": psutil_process_time_user,
             "psutil_process_time_system": psutil_process_time_system,
             "psutil_process_memory_virtual": psutil_process_memory_virtual,
             "psutil_process_memory_resident": psutil_process_memory_resident,
             "psutil_process_disk_read": psutil_process_disk_read,
             "psutil_process_disk_write": psutil_process_disk_write,
             "psutil_process_status": psutil_process_status,
             "psutil_cpu_num": psutil_cpu_num,
             "psutil_process_num_ctx_switches_voluntary": psutil_process_num_ctx_switches_voluntary,
             "psutil_process_num_ctx_switches_involuntary": psutil_process_num_ctx_switches_involuntary
             }
        events.append(d)

    connection.close()

    return events
