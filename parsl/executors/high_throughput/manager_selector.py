import random
import copy


class ManagerSelectorBase:

    pass


class ManagerSelectorRandom(ManagerSelectorBase):

    def __init__(self):
        pass

    def sort_managers(self, manager_list: list[str]) -> list[str]:
        c_manager_list = copy.copy(manager_list)
        random.shuffle(c_manager_list)
        return c_manager_list
