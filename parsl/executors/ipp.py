from parsl.executors.base import ParslExecutor


class IPyParallelExecutor(ParslExecutor):
    """This stub exists to issue a more helpful warning about the IPyParallel
    executor being removed from parsl some time after v0.9.

    It can eventually be removed entirely - perhaps after v0.10
    """

    def __new__(*args, **kwargs):
        raise RuntimeError("The IPyParallel executor has been removed from parsl")
