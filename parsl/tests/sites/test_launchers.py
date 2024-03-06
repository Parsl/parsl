from parsl.launchers import WrappedLauncher


def test_wrapped_launcher(caplog):
    w = WrappedLauncher('docker')
    assert w('test', 1, 1) == 'docker test'

    # Make sure it complains if you have > 1 node or task per node
    w('test', 2, 2)
    assert 'tasks per node' in caplog.text
    assert 'nodes per block' in caplog.text
