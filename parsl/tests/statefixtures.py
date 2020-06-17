import parsl
import pytest

from parsl.dataflow.states import FINAL_STATES


@pytest.fixture(autouse=True)
def prohibit_active_tasks():
    yield
    if parsl.dataflow.dflow.DataFlowKernelLoader._dfk:
        dfk = parsl.dataflow.dflow.DataFlowKernelLoader._dfk

        for task_record in dfk.tasks.values():
            if task_record['status'] in FINAL_STATES:
                pass
            else:
                raise RuntimeError("parsl test environment prohibits active tasks between tests, but task {} is active".format(task_record['id']))
    else:
        pass  # if there's no global DFK then we don't care
