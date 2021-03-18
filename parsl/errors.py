from parsl.app.errors import ParslError

from typing import List


class OptionalModuleMissing(ParslError):
    ''' Error raised when a required module is missing for a optional/extra component
    '''

    def __init__(self, module_names: List[str], reason: str, bt=None):
        self.module_names = module_names
        self.reason = reason
        self.bt = bt

    def __str__(self) -> str:
        return "The functionality requested requires missing optional modules {0}, because: {1} bt={2}".format(
            self.module_names, self.reason, self.bt
        )
