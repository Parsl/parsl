import pytest

import parsl
import time
from parsl import python_app, ThreadPoolExecutor
from parsl.config import Config
from parsl.data_provider.files import File
from parsl.data_provider.staging import Staging


@python_app
def observe_input_local_path(f):
    """Returns the local_path that is seen by the app,
    so that the test suite can also see that path.
    """
    return f.local_path


@python_app
def wait_and_create(outputs=[]):
    # for test purposes, this doesn't actually need to create the output
    # file as nothing ever touches file content - the test only deals with
    # names and Futures.
    time.sleep(10)


class SP2(Staging):
    def can_stage_in(self, file):
        return file.scheme == 'sp2'

    def stage_in(self, dm, executor, file, parent_fut):
        file.local_path = "./test1.tmp"
        return None  # specify no tasks inserted in graph so parsl will preserve stageout dependency

    def can_stage_out(self, file):
        return file.scheme == 'sp2'

    def stage_out(self, dm, executor, file, app_fu):
        file.local_path = "./test1.tmp"
        return None  # no tasks inserted so parsl should use app completion for DataFuture completion


@pytest.mark.local
def test_1316_local_path_on_execution_side_sp2():
    """This test demonstrates the ability of a StagingProvider to set the
    local_path of a File on the execution side, but that the change does not
    modify the local_path of the corresponding submit side File, even when
    running in a single python process.
    """

    config = Config(executors=[ThreadPoolExecutor(storage_access=[SP2()])])

    file = File("sp2://test")

    parsl.load(config)
    p = observe_input_local_path(file).result()

    assert p == "./test1.tmp", "File object on the execution side gets the local_path set by the staging provider"

    assert not file.local_path, "The local_path on the submit side should not be set"

    parsl.clear()


@pytest.mark.local
def test_1316_local_path_setting_preserves_dependency_sp2():
    config = Config(executors=[ThreadPoolExecutor(storage_access=[SP2()])])

    file = File("sp2://test")

    parsl.load(config)

    wc_app_future = wait_and_create(outputs=[file])
    data_future = wc_app_future.outputs[0]

    p = observe_input_local_path(data_future).result()

    assert wc_app_future.done(), "wait_and_create should finish before observe_input_local_path finishes"

    assert p == "./test1.tmp", "File object on the execution side gets the local_path set by the staging provider"

    assert not file.local_path, "The local_path on the submit side should not be set"

    parsl.clear()
