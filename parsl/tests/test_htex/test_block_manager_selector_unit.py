import pytest

from parsl.executors.high_throughput.manager_record import ManagerRecord
from parsl.executors.high_throughput.manager_selector import BlockIdManagerSelector


@pytest.mark.local
def test_sort_managers():
    ready_managers = {
        b'manager1': {'block_id': 1},
        b'manager2': {'block_id': None},
        b'manager3': {'block_id': 3},
        b'manager4': {'block_id': 2}
    }

    manager_list = {b'manager1', b'manager2', b'manager3', b'manager4'}
    expected_sorted_list = [b'manager2', b'manager1', b'manager4', b'manager3']
    manager_selector = BlockIdManagerSelector()
    sorted_managers = manager_selector.sort_managers(ready_managers, manager_list)
    assert sorted_managers == expected_sorted_list
