from typing import Callable, Sequence

# TODO: note the different in return/edit semantics of
# widen_one vs widen_dict. for historical reasons, not
# because its a good idea. one returns new log records,
# the other modifies existing log records.


def widen_one(log: list, key: str, val: object) -> list:
    return [d | {key: val} for d in log]


def widen_dict(log: list, d: dict) -> None:
    for lr in log:
        lr.update(d)


def widen_dict_without_overwrite(log: list, d: dict) -> None:
    """Like widen_dict but raises an error if any key would be overwritten.
    A key can be updated by the dict to the same value without error.
    """
    for lr in log:
        for k, v in d.items():
            assert k not in lr or lr[k] == v, f"Key {k} tries to update {lr[k]} to {v}"
            lr[k] = v


def widen_lambda(log: list[dict], f: Callable[dict, dict]) -> None:
    for lr in log:
        d = f(lr)
        lr.update(d)


def get_unique(logs, key) -> object:
    """Returns the unique value of key across all log lines.

    For example, a run ID that applies to the whole log file and might be
    mentioned in several/many log records.

    Raise an error if the key does not have a unique value, because that
    means there is probably a misassumption by the caller about what the
    data looks like.
    """
    vs = {lr[key]
          for lr in logs
          if key in lr}
    vs = get_all_values(key)
    assert len(vs) == 1, f"Supposedly unique key {key} has multiple values: {vs}"
    return list(vs)[0]


def get_all_values(logs, key) -> set:
    """Returns all values of the key across all log lines.

    get_unique will return the value of this set if the set has one
    element and fail otherwise.
    """
    return {lr[key]
            for lr in logs
            if key in lr}


def group_by_keys(logs, keys: Sequence):
    """returns a dictionary where each entry value is a collection of
    log records that all have the same values on all of the keys,
    keyed by a sequence of those values.
    discards records which do not have a value set on all of the keys.
    """

    tgroups = {}  # maybe a 'defaultdict' would be better for this?
    for lr in logs:
        l_key = ()
        missing = False
        for k in keys:
            if k not in lr:
                missing = True
            else:
                l_key = (l_key, lr[k])

        if not missing:
            if l_key not in tgroups:
                tgroups[l_key] = []
            tgroups[l_key].append(lr)  # group, don't reshape the log entry
    return tgroups


def widen_by_implication(logs, keys: Sequence, implied_key):
    """widens records by implication: if any record identified by the key sequence
    contains an implied_key, then all records identified by that key sequence are
    widened to contain that implied key: keys => implied_key"""

    tgroups = group_by_keys(logs, keys)

    for k, tg in tgroups.items():
        r_key_vals = get_all_values(tg, implied_key)

        assert len(r_key_vals) == 0 or len(r_key_vals) == 1, "implication cannot imply multiple values"

        if len(r_key_vals) == 1:
            unique_r_key_val = list(r_key_vals)[0]
            for lr in tg:
                assert implied_key not in lr or lr[implied_key] == unique_r_key_val, "implication cannot modify implied key"
                lr[implied_key] = unique_r_key_val
