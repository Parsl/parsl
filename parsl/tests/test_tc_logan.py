import parsl
import pytest

from parsl.dataflow.taskrecord import TaskRecord


def l(t: TaskRecord):
    t['kwargs']['s'] = f"hi, magic value for app {t['func_name']} on executor {t['executor']}"


@parsl.bash_app
def write(s: str, stdout=parsl.AUTO_LOGNAME):
    return f"echo {s}"


@pytest.mark.local
def test_tc_logan():
    parsl.load(parsl.Config(tc_logan=l))
    f = write()
    f.result()
    parsl.dfk().cleanup()
