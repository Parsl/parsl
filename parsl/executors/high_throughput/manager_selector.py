import random
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set

from parsl.executors.high_throughput.manager_record import ManagerRecord


class ManagerSelector(metaclass=ABCMeta):

    @abstractmethod
    def sort_managers(self, ready_managers: Dict[bytes, ManagerRecord], manager_list: Set[bytes]) -> List[bytes]:
        """ Sort a given list of managers.

        Any operations pertaining to the sorting and rearrangement of the
        interesting_managers Set should be performed here.
        """
        pass


class RandomManagerSelector(ManagerSelector):

    """Returns a shuffled list of interesting_managers

    By default this strategy is used by the interchange. Works well
    in distributing workloads equally across all availble compute
    resources. The random workload strategy is not effective in
    conjunction with elastic scaling behavior as the even task
    distribution does not allow the scaling down of blocks, leading
    to wasted resource consumption.
    """

    def sort_managers(self, ready_managers: Dict[bytes, ManagerRecord], manager_list: Set[bytes]) -> List[bytes]:
        c_manager_list = list(manager_list)
        random.shuffle(c_manager_list)
        return c_manager_list


class BlockIdManagerSelector(ManagerSelector):

    """Returns an interesting_managers list sorted by block ID

    Observations:
    1. BlockID manager selector helps with workloads that see a varying
    amount of tasks over time. New blocks are prioritized with the
    blockID manager selector, when used with 'htex_auto_scaling', results
    in compute cost savings.

    2. Doesn't really work with bag-of-tasks workloads. When all the tasks
    are put into the queue upfront, all blocks operate at near full
    utilization for the majority of the workload, which task goes where
    doesn't really matter.
    """

    def sort_managers(self, ready_managers: Dict[bytes, ManagerRecord], manager_list: Set[bytes]) -> List[bytes]:
        return sorted(manager_list, key=lambda x: (ready_managers[x]['block_id'] is not None, ready_managers[x]['block_id']))
