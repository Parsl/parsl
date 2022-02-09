from parsl.app.errors import ParslError

from typing import List


class OptionalModuleMissing(ParslError):
    ''' Error raised when a required module is missing for a optional/extra component
    '''

    def __init__(self, module_names: List[str], reason: str):
        self.module_names = module_names
        self.reason = reason

    def __str__(self) -> str:
        return "The functionality requested requires optional modules {0} which could not be imported, because: {1}".format(
            self.module_names, self.reason
        )
