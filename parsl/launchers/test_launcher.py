# test_launcher.py
from parsl.launchers import SimpleLauncher

def test_single_node_per_block():
    launcher = SimpleLauncher(nodes_per_block=1)
    assert launcher.nodes_per_block == 1

def test_exception_raised_for_multiple_nodes_per_block():
    try:
        launcher = SimpleLauncher(nodes_per_block=64)
    except Exception as e:
        assert str(e) == "SimpleLauncher only supports 1 node per block by default."
    else:
        assert False, "Expected an exception but none was raised."

def test_permit_multiple_nodes():
    try:
        launcher = SimpleLauncher(nodes_per_block=64, permit_multiple_nodes=True)
    except Exception as e:
        assert False, f"Expected no exception but got: {str(e)}"
    else:
        assert launcher.nodes_per_block == 64
