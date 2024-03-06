from parsl.launchers import WrappedLauncher


def test_wrapped_launcher(caplog):
    w = WrappedLauncher('docker')
    assert w('test', 1, 1) == 'docker test'

    # Make sure it complains if you have > 1 node or task per node
    w('test', 2, 2)
    assert 'tasks per node' in caplog.text
    assert 'nodes per block' in caplog.text
    
def test_simple_launcher_one_node_per_block():
    # Test that SimpleLauncher works with one node per block specified
    launcher = SimpleLauncher()
    assert launcher("", 1, 1) == ""

def test_simple_launcher_exception():
    # Test that SimpleLauncher raises an exception with some other number of nodes per block
    launcher = SimpleLauncher()
    with pytest.raises(ValueError):
        launcher("", 1, 64)

def test_advanced_users_permit_multiple_nodes():
    # Test that advanced users can disable the exception when using multiple nodes per block
    launcher = SimpleLauncher(permit_multiple_nodes=True)
    assert launcher("", 1, 64) == ""