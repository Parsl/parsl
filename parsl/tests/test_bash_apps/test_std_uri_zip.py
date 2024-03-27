import parsl
import pytest

from functools import partial

from parsl.data_provider.files import File

from parsl.tests.configs.htex_local_alternate import fresh_config


@parsl.bash_app
def app(stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    return "echo hello"


def generate_task_name(tmpd_cwd, task_record, err_or_out):
    task_id = task_record['id']
    func_name = task_record['func_name']
    zipbase = str(int(task_id / 10)).zfill(3)
    zipfile = f"{tmpd_cwd}/stored_results_{zipbase}.zip"
    return File(f"zip:{zipfile}/{func_name}.{task_id}.{err_or_out}")


@pytest.mark.local
def test_std_uri(tmpd_cwd):
    c = fresh_config()
    c.std_uri = partial(generate_task_name, tmpd_cwd)
    with parsl.load(c):
        f = app()
        f.stdout_future.result()
        f.stderr_future.result()
    parsl.clear()


@pytest.mark.local
def test_std_uri_many(tmpd_cwd):
    c = fresh_config()
    c.std_uri = partial(generate_task_name, tmpd_cwd)
    fs = []
    with parsl.load(c):
        for n in range(0, 30):
            fs.append(app())

        for f in fs:
            f.stdout_future.result()
            f.stderr_future.result()
    parsl.clear()
