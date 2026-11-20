from ..util import Combo, interoperable


def compatible(combo: Combo, oldest: str) -> bool:
    # apps are serialized with dill, whose output isn't portable across Python minor versions
    return interoperable(combo, oldest) and combo.submit_env.python == combo.worker_env.python
