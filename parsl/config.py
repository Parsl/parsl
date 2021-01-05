import logging
import typeguard

from typing import List, Optional

from parsl.utils import RepresentationMixin
from parsl.executors.base import ParslExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.dataflow.error import ConfigurationError
from parsl.monitoring import MonitoringHub

logger = logging.getLogger(__name__)


class Config(RepresentationMixin):
    """
    Specification of Parsl configuration options.

    Parameters
    ----------
    executors : list of ParslExecutor, optional
        List of `ParslExecutor` instances to use for executing tasks.
        Default is [:class:`~parsl.executors.threads.ThreadPoolExecutor()`].
    app_cache : bool, optional
        Enable app caching. Default is True.
    checkpoint_files : list of str, optional
        List of paths to checkpoint files. See :func:`parsl.utils.get_all_checkpoints` and
        :func:`parsl.utils.get_last_checkpoint` for helpers. Default is None.
    checkpoint_mode : str, optional
        Checkpoint mode to use, can be ``'dfk_exit'``, ``'task_exit'``, or ``'periodic'``. If set to
        `None`, checkpointing will be disabled. Default is None.
    checkpoint_period : str, optional
        Time interval (in "HH:MM:SS") at which to checkpoint completed tasks. Only has an effect if
        ``checkpoint_mode='periodic'``.
    garbage_collect : bool. optional.
        Delete task records from DFK when tasks have completed. Default: True
    internal_tasks_max_threads : int, optional
        Maximum number of threads to allocate for submit side internal tasks such as some data transfers
        or @joinapps
        Default is 10.
    monitoring : MonitoringHub, optional
        The config to use for database monitoring. Default is None which does not log to a database.
    retries : int, optional
        Set the number of retries in case of failure. Default is 0.
    run_dir : str, optional
        Path to run directory. Default is 'runinfo'.
    strategy : str, optional
        Strategy to use for scaling resources according to workflow needs. Can be 'simple' or `None`. If `None`, dynamic
        scaling will be disabled. Default is 'simple'.
    max_idletime : float, optional
        The maximum idle time allowed for an executor before strategy could shut down unused resources (scheduler jobs). Default is 120.0 seconds.
    usage_tracking : bool, optional
        Set this field to True to opt-in to Parsl's usage tracking system. Parsl only collects minimal, non personally-identifiable,
        information used for reporting to our funding agencies. Default is False.
    initialize_logging : bool, optional
        Make DFK optionally not initialize any logging. Log messages
        will still be passed into the python logging system under the
        ``parsl`` logger name, but the logging system will not by default
        perform any further log system configuration. Most noticeably,
        it will not create a parsl.log logfile.  The use case for this
        is when parsl is used as a library in a bigger system which
        wants to configure logging in a way that makes sense for that
        bigger system as a whole.
    """

    @typeguard.typechecked
    def __init__(self,
                 executors: Optional[List[ParslExecutor]] = None,
                 app_cache: bool = True,
                 checkpoint_files: Optional[List[str]] = None,
                 checkpoint_mode: Optional[str] = None,
                 checkpoint_period: Optional[str] = None,
                 garbage_collect: bool = True,
                 internal_tasks_max_threads: int = 10,
                 retries: int = 0,
                 run_dir: str = 'runinfo',
                 strategy: Optional[str] = 'simple',
                 max_idletime: float = 120.0,
                 monitoring: Optional[MonitoringHub] = None,
                 usage_tracking: bool = False,
                 initialize_logging: bool = True):
        if executors is None:
            executors = [ThreadPoolExecutor()]
        self.executors = executors
        self.app_cache = app_cache
        self.checkpoint_files = checkpoint_files
        self.checkpoint_mode = checkpoint_mode
        if checkpoint_period is not None:
            if checkpoint_mode is None:
                logger.debug('The requested `checkpoint_period={}` will have no effect because `checkpoint_mode=None`'.format(
                    checkpoint_period)
                )
            elif checkpoint_mode != 'periodic':
                logger.debug("Requested checkpoint period of {} only has an effect with checkpoint_mode='periodic'".format(
                    checkpoint_period)
                )
        if checkpoint_mode == 'periodic' and checkpoint_period is None:
            checkpoint_period = "00:30:00"
        self.checkpoint_period = checkpoint_period
        self.garbage_collect = garbage_collect
        self.internal_tasks_max_threads = internal_tasks_max_threads
        self.retries = retries
        self.run_dir = run_dir
        self.strategy = strategy
        self.max_idletime = max_idletime
        self.usage_tracking = usage_tracking
        self.initialize_logging = initialize_logging
        self.monitoring = monitoring

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
