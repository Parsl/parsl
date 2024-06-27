from concurrent.futures import Future
import pytest
#from pytest_mock import mocker

from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.dynamic_files import DynamicFileList as DFL
from parsl.data_provider.files import File
from parsl.app.futures import DataFuture
from parsl.dataflow.futures import AppFuture


class MockDFK:
    def __init__(self, tasks):
        self.executors = {'tester': 'test'}
        self.storage_access = []
        self.data_manager = DataManager(self)
        self.tasks = {tasks['id']: tasks}


def test_dynamic_file_empty():
    fut = DFL()
    df = DFL.DynamicFile(fut, None)
    assert df.file_obj is None
    assert df.empty is True


def test_dynamic_file_File():
    fut = DFL()
    fl = File('test.txt')
    df = DFL.DynamicFile(fut, fl)
    assert df.file_obj == fl
    assert df.empty is False
    assert df.done() is True
    assert df.filename.endswith('test.txt')


def test_dynamic_file_DataFuture():
    fut = Future()
    fl = DataFuture(fut, File('test2.txt'))
    df = DFL.DynamicFile(DFL(), fl)
    assert df.file_obj == fl
    assert df.empty is False
    assert df.done() is False
    assert df.tid is None
    assert df.filepath.endswith('test2.txt')


def test_dynamic_file_set():
    fut = DFL()
    df = DFL.DynamicFile(fut, None)
    assert df.file_obj is None
    assert df.empty is True
    fl = File('test.txt')
    df.set(fl)
    assert df.file_obj == fl
    assert df.empty is False


def test_dynamic_file_not_implemented():
    fut = DFL()
    fl = DataFuture(fut, File('test2.txt'))
    df = DFL.DynamicFile(fut, fl)
    with pytest.raises(NotImplementedError):
        df.cancel()
    assert df.cancelled() is False
    assert df.exception() is None


def test_dynamic_file_passthrough():
    fut = DFL()
    dfl = DataFuture(fut, File('test3.txt'))
    df = DFL.DynamicFile(fut, dfl)
    assert df.running() is False
    af = AppFuture({})
    fut.set_parent(af)
    assert df.running() is False
    af.set_running_or_notify_cancel()
    assert df.running() is True


def test_dynamic_file_callback():
    fut = DFL()
    dfl = DataFuture(fut, File('test3.txt'))
    df = DFL.DynamicFile(fut, dfl)
    with pytest.raises(Exception):
        df.result(1)
    fut.set_running_or_notify_cancel()
    fut.set_result(5)
    assert df.done() is True
    assert isinstance(df.result(), Future)


def test_dynamic_file_list():
    dfl = DFL()
    assert len(dfl) == 0


def test_dynamic_file_list_append():
    dfl = DFL()
    assert len(dfl) == 0

    dfl.append(File('test.txt'))
    dfl.append(File('test2.txt'))
    assert len(dfl) == 2


def test_dynamic_file_list_extend():
    dfl = DFL()
    assert len(dfl) == 0

    dfl.extend([File('test.txt'), File('test2.txt')])
    assert len(dfl) == 2


def test_dynamic_file_list_dynamics():
    dfl = DFL()
    assert len(dfl) == 0

    dfl[5] = File('test4.txt')
    assert len(dfl) == 6

    tempid = id(dfl[3])
    dfl[3] = File('test3.txt')
    assert len(dfl) == 6
    assert id(dfl[3]) == tempid

    dfl[2] = dfl[5]
    assert len(dfl) == 6
    assert id(dfl[2]) != tempid
    tempid = id(dfl[5])
    with pytest.raises(ValueError):
        dfl[5] = File('another.test.dat')
    assert len(dfl) == 6
    assert tempid == id(dfl[5])


def test_dynamic_file_list_no_op():
    dfl = DFL()
    with pytest.raises(Exception):
        dfl.cancel()
    assert dfl.cancelled() is False


def test_dynamic_file_list_dynamic_append():
    dfl = DFL()
    _ = dfl[5]
    assert len(dfl) == 6
    f0 = File('tester5.dat')
    dfl[0] = f0
    assert dfl[1].empty
    tempid = id(dfl[1])
    f1 = File('tester2.dat')
    dfl.append(f1)
    assert len(dfl) == 6
    assert dfl[0].filename == f0.filename
    assert dfl[1].filename == f1.filename
    assert tempid == id(dfl[1])
    assert dfl[2].empty


def test_dynamic_file_list_dynamic_extend():
    dfl = DFL()
    _ = dfl[5]
    assert len(dfl) == 6
    f0 = File('tester5.dat')
    dfl[0] = f0
    assert dfl[1].empty
    tempids = [id(dfl[0]), id(dfl[1]), id(dfl[2]), id(dfl[3])]
    f1 = File('tester2.dat')
    f2 = File('tester3.dat')
    dfl.extend([f1, f2])
    assert len(dfl) == 6
    assert dfl[0].filename == f0.filename
    assert tempids[0] == id(dfl[0])
    assert dfl[1].filename == f1.filename
    assert tempids[1] == id(dfl[1])
    assert dfl[2].filename == f2.filename
    assert tempids[2] == id(dfl[2])
    assert dfl[3].empty
    assert tempids[3] == id(dfl[3])
    f3 = File('tester4.dat')
    f4 = File('tester5.dat')
    f5 = File('tester6.dat')
    f6 = File('tester7.dat')
    dfl.extend([f3, f4, f5, f6])
    assert len(dfl) == 7


def test_dynamic_file_list_insert_and_remove():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    dfl.insert(4, File('tester99.dat'))
    assert len(dfl) == 11
    assert dfl[4].filename == 'tester99.dat'
    assert dfl[5].filename == 'tester4.dat'

    dfl.remove(dfl[5])
    assert len(dfl) == 10
    assert dfl[4].filename == 'tester99.dat'
    assert dfl[5].filename == 'tester5.dat'

    dfl.append(File('tester100.dat'))
    assert len(dfl) == 11


def test_dynamic_file_list_clear():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    dfl.clear()
    assert len(dfl) == 0


def test_dynamic_file_list_pop():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    assert dfl.pop().filename == 'tester9.dat'
    assert len(dfl) == 9
    assert dfl.pop(3).filename == 'tester3.dat'
    assert len(dfl) == 8
    _ = dfl[15]
    dfl.append(File('tester10.dat'))
    assert dfl[0].filename == 'tester0.dat'
    assert dfl[7].filename == 'tester8.dat'
    assert dfl[8].filename == 'tester10.dat'
    assert dfl[9].empty
    assert dfl._last_idx == 8
    assert len(dfl) == 16
    assert dfl.pop().filename == 'tester10.dat'


def test_dynamic_file_sub_list_fixed_size():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    dfsl = dfl[2:5]
    assert len(dfsl) == 3
    assert dfsl[0].filename == 'tester2.dat'
    assert dfsl[2].filename == 'tester4.dat'
    dfl.insert(3, File('testBig.dat'))
    assert len(dfl) == 11
    assert len(dfsl) == 3
    assert dfsl[0].filename == 'tester2.dat'
    assert dfsl[1].filename == 'testBig.dat'
    assert dfsl[2].filename == 'tester3.dat'

    dfl.insert(0, File('testSmall.dat'))
    assert len(dfl) == 12
    assert len(dfsl) == 3
    assert dfsl[0].filename == 'tester1.dat'
    assert dfsl[1].filename == 'tester2.dat'
    assert dfsl[2].filename == 'testBig.dat'

    dfl.append(File('testBig3.dat'))
    assert len(dfl) == 13
    assert len(dfsl) == 3
    assert dfsl[0].filename == 'tester1.dat'
    assert dfsl[1].filename == 'tester2.dat'
    assert dfsl[2].filename == 'testBig.dat'


def test_dynamic_file_sub_list_dynamic_size_upper():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    dfsl = dfl[2:]
    assert len(dfsl) == 8
    dfl.insert(3, File('testBig.dat'))
    assert len(dfl) == 11
    assert len(dfsl) == 9
    assert dfsl[0].filename == 'tester2.dat'
    assert dfsl[1].filename == 'testBig.dat'
    assert dfsl[2].filename == 'tester3.dat'

    dfl.insert(0, File('testSmall.dat'))
    assert len(dfl) == 12
    assert len(dfsl) == 10
    assert dfsl[0].filename == 'tester1.dat'
    assert dfsl[1].filename == 'tester2.dat'
    assert dfsl[2].filename == 'testBig.dat'

    dfl.append(File('testBig3.dat'))
    assert len(dfl) == 13
    assert len(dfsl) == 11
    assert dfsl[0].filename == 'tester1.dat'
    assert dfsl[1].filename == 'tester2.dat'
    assert dfsl[2].filename == 'testBig.dat'


def test_dynamic_file_sub_list_dynamic_size_lower():
    f = [File(f'tester{i}.dat') for i in range(10)]
    dfl = DFL(f)
    assert len(dfl) == 10
    dfsl = dfl[:5]
    assert len(dfsl) == 5
    dfl.insert(3, File('testBig.dat'))
    assert len(dfl) == 11
    assert len(dfsl) == 5
    assert dfsl[2].filename == 'tester2.dat'
    assert dfsl[3].filename == 'testBig.dat'
    assert dfsl[4].filename == 'tester3.dat'

    dfl.insert(0, File('testSmall.dat'))
    assert len(dfl) == 12
    assert len(dfsl) == 5
    assert dfsl[0].filename == 'testSmall.dat'
    assert dfsl[1].filename == 'tester0.dat'
    assert dfsl[2].filename == 'tester1.dat'

    dfl.append(File('testBig3.dat'))
    assert len(dfl) == 13
    assert len(dfsl) == 5
    assert dfsl[0].filename == 'testSmall.dat'
    assert dfsl[1].filename == 'tester0.dat'
    assert dfsl[2].filename == 'tester1.dat'


def test_dynamic_file_sub_list_dynamic_size_creation():
    dfl = DFL()
    assert len(dfl) == 0
    dfsl = dfl[2:]
    assert len(dfl) == 3
    assert len(dfsl) == 1

    dfl2 = DFL()
    assert len(dfl2) == 0
    dfsl2 = dfl2[2:5]
    assert len(dfl2) == 5
    assert len(dfsl2) == 3

    dfl3 = DFL()
    assert len(dfl3) == 0
    dfsl3 = dfl3[:5]
    assert len(dfl3) == 5
    assert len(dfsl3) == 5


def test_dynamic_file_list_set_parent():
    dfl = DFL()
    assert dfl.parent is None
    af = AppFuture({})
    dfl.set_parent(af)
    assert dfl.parent == af
    af1 = AppFuture({})
    with pytest.raises(ValueError):
        dfl.set_parent(af1)


def test_dynamic_file_list_set_dataflow():
    dfl = DFL()
    tr = {'id': 101, 'func': None}
    af = AppFuture(tr)
    dfl.set_parent(af)
    dflow = MockDFK(tr)
    dfl.set_dataflow(dflow, 'tester', False, 101)
    assert dfl.dataflow == dflow
    assert dfl.parent._outputs == dfl


def test_dynamic_file_list_staging_with_files():
    dfl = DFL([File(f'tester{i}.dat') for i in range(10)])
    tr = {'id': 101, 'func': None}
    af = AppFuture(tr)
    dfl.set_parent(af)
    dflow = MockDFK(tr)
    dfl.set_dataflow(dflow, 'tester', False, 101)
    assert dfl.dataflow == dflow
    assert dfl.parent._outputs == dfl


def test_dynamic_file_list_staging_with_df():
    tr = {'id': 101, 'func': None}
    af = AppFuture(tr)
    dfl = DFL([DataFuture(af, File(f'tester{i}.dat')) for i in range(10)])
    dfl.set_parent(af)
    dflow = MockDFK(tr)
    dfl.set_dataflow(dflow, 'tester', False, 101)
    assert dfl.dataflow == dflow
    assert dfl.parent._outputs == dfl


def test_dynamic_filer_list_staging_mix():
    dfl = DFL([File(f'tester{i}.dat') for i in range(10)])
    tr = {'id': 101, 'func': None}
    af = AppFuture(tr)
    dfl.set_parent(af)
    dflow = MockDFK(tr)
    dfl.set_dataflow(dflow, 'tester', False, 101)
    assert dfl.dataflow == dflow
    assert dfl.parent._outputs == dfl
    dfl.append(File('newtester.dat'))


def stub_func():
    pass


def test_dynamic_file_list_mock(mocker):
    path = 'parsl.data_provider.data_manager.DataManager'
    mocker.patch(f"{path}.stage_out", return_value=stub_func)
    dfl = DFL([File(f'tester{i}.dat') for i in range(10)])
    tr = {'id': 101, 'func': None}
    af = AppFuture(tr)
    dfl.set_parent(af)
    dflow = MockDFK(tr)
    dfl.set_dataflow(dflow, 'tester', False, 101)
    assert dfl.dataflow == dflow
    assert dfl.parent._outputs == dfl
    dfl.append(File('newtester.dat'))


if __name__ == '__main__':
    ret = pytest.main(['-v', __file__,  '--config', '/home/friedel/devel/parsl/parsl/tests/configs/local_threads.py', '--random-order'])