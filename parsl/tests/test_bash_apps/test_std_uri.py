import parsl
import pytest


@parsl.bash_app
def app(stdout=parsl.AUTO_LOGNAME):
    return "echo hello"


def generate_task_name(task_record, err_or_out):
    return f"BENC_stdx.{task_record['id']}.{err_or_out}"


@pytest.mark.local
def test_std_uri():
    with parsl.load(parsl.Config(std_uri=generate_task_name)):
        app().result()
