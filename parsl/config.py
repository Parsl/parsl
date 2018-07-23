import logging

from libsubmit.utils import RepresentationMixin
from parsl.executors.threads import ThreadPoolExecutor
from parsl.dataflow.error import ConfigurationError

from typing import Optional, Any, List # for mypy
from parsl.executors.base import ParslExecutor # for mypy

logger = logging.getLogger(__name__)


class Config(RepresentationMixin):
    """
    Specification of Parsl configuration options.

    Parameters
    ----------
    executors : list of ParslExecutor, optional
        List of executor instances to use. Possible executors include :class:`~parsl.executors.threads.ThreadPoolExecutor`,
        :class:`~parsl.executors.ipp.IPyParallelExecutor`, or :class:`~parsl.executors.swift_t.TurbineExecutor`. Default
        is [:class:`~parsl.executors.threads.ThreadPoolExecutor()`].
    app_cache : bool, optional
        Enable app caching. Default is True.
    checkpoint_files : list of str, optional
        List of paths to checkpoint files. Default is None.
    checkpoint_mode : str, optional
        Checkpoint mode to use, can be 'dfk_exit', 'task_exit', or 'periodic'. If set to
        `None`, checkpointing will be disabled. Default is None.
    checkpoint_period : str, optional
        Time interval (in "HH:MM:SS") at which to checkpoint completed tasks. Only has an effect if
        `checkpoint_mode='periodic'`.
    data_management_max_threads : int, optional
        Maximum number of threads to allocate for the data manager to use for managing input and output transfers.
        Default is 10.
    lazy_errors : bool, optional
        If True, errors from task failures will not be raised until `future.result()` is called. Otherwise, they will
        be raised as soon as the task returns. Default is True.
    retries : int, optional
        Set the number of retries in case of failure. Default is 0.
    run_dir : str, optional
        Path to run directory. Default is 'runinfo'.
    strategy : str, optional
        Strategy to use for scaling resources according to workflow needs. Can be 'simple' or `None`. If `None`, dynamic
        scaling will be disabled. Default is 'simple'.
    # TODO: db_logger_config is not documented here. The type is keyword based here, by the looks of it, which might
    # be better structured as a type-checked config object? For now, I've told mypy it is Optional[Any] which is weak.
    usage_tracking : bool, optional
        Enable usage tracking. Default is True.
    """
    def __init__(self,
                 executors=None,
                 app_cache=True,
                 checkpoint_files=None,
                 checkpoint_mode=None,
                 checkpoint_period="00:30:00",
                 data_management_max_threads=10,
                 lazy_errors=True,
                 retries=0,
                 run_dir='runinfo',
                 strategy='simple',
                 db_logger_config=None,
                 usage_tracking=True):
        # type: (Optional[List[ParslExecutor]], Optional[bool], Optional[List[str]], Optional[str], Optional[str], Optional[int], Optional[bool], Optional[int], Optional[str], Optional[str], Optional[Any], Optional[bool]) -> None
        if executors is None:
            executors = [ThreadPoolExecutor()]
        self.executors = executors
        self.app_cache = app_cache
        self.checkpoint_files = checkpoint_files
        self.checkpoint_mode = checkpoint_mode
        if checkpoint_mode is not 'periodic' and checkpoint_period is not None:
            logger.debug("Checkpoint period only has an effect with checkpoint_mode='periodic'")
        self.checkpoint_period = checkpoint_period
        self.data_management_max_threads = data_management_max_threads
        self.lazy_errors = lazy_errors
        self.retries = retries
        self.run_dir = run_dir
        self.strategy = strategy
        self.usage_tracking = usage_tracking
        self.db_logger_config = db_logger_config

    @property
    def executors(self):
        return self._executors

    @executors.setter
    def executors(self, executors):
        labels = [e.label for e in executors]
        duplicates = [e for n, e in enumerate(labels) if e in labels[:n]]
        if len(duplicates) > 0:
            raise ConfigurationError('Executors must have unique labels ({})'.format(
                ', '.join(['label={}'.format(repr(d)) for d in duplicates])))
        self._executors = executors
