import copy
import random
from typing import Dict, List

from parsl.executors.high_throughput.manager_record import ManagerRecord


class ManagerSelectorBase:

    def sort_managers(self, ready_managers: Dict[bytes, ManagerRecord], manager_list: List[bytes]) -> List[bytes]:
        raise NotImplementedError


class ManagerSelectorRandom(ManagerSelectorBase):

    def __init__(self):
        pass

    def sort_managers(self, ready_managers: Dict[bytes, ManagerRecord], manager_list: List[bytes]) -> List[bytes]:
        c_manager_list = copy.copy(manager_list)
        random.shuffle(c_manager_list)
        return c_manager_list
