from ..util import HERE, Combo, supported


def compatible(combo: Combo, oldest: str) -> bool:
    return combo.submit_env.parsl == HERE and not supported(combo.worker_env, oldest)
