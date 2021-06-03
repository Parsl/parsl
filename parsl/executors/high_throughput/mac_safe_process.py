import multiprocessing
from typing import Any

ForkProcess: Any = multiprocessing.get_context('fork').Process


class MacSafeProcess(ForkProcess):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
