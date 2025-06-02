import logging
from typing import Callable, Iterable, Optional, Sequence, Union

import typeguard
from typing_extensions import Literal

from parsl.dataflow.dependency_resolvers import DependencyResolver
from parsl.dataflow.taskrecord import TaskRecord
from parsl.errors import ConfigurationError
from parsl.executors.base import ParslExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.monitoring import MonitoringHub
from parsl.usage_tracking.api import UsageInformation
from parsl.usage_tracking.levels import DISABLED as USAGE_TRACKING_DISABLED
from parsl.usage_tracking.levels import LEVEL_3 as USAGE_TRACKING_LEVEL_3
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class Config(RepresentationMixin, UsageInformation):
    """
    Specification of Parsl configuration options.

    Parameters
    ----------
    executors : sequence of ParslExecutor, optional
        List (or other iterable) of `ParslExecutor` instances to use for executing tasks.
        Default is (:class:`~parsl.executors.threads.ThreadPoolExecutor()`,).
    app_cache : bool, optional
        Enable app caching. Default is True.
    checkpoint_files : sequence of str, optional
        List of paths to checkpoint files. See :func:`parsl.utils.get_all_checkpoints` and
        :func:`parsl.utils.get_last_checkpoint` for helpers. Default is None.
    checkpoint_mode : str, optional
        Checkpoint mode to use, can be ``'dfk_exit'``, ``'task_exit'``, ``'periodic'`` or ``'manual'``.
        If set to `None`, checkpointing will be disabled. Default is None.
    checkpoint_period : str, optional
        Time interval (in "HH:MM:SS") at which to checkpoint completed tasks. Only has an effect if
        ``checkpoint_mode='periodic'``.
    dependency_resolver: plugin point for custom dependency resolvers. Default: only resolve Futures,
        using the `SHALLOW_DEPENDENCY_RESOLVER`.
    exit_mode: str, optional
        When Parsl is used as a context manager (using ``with parsl.load`` syntax) then this parameter
        controls what will happen to running tasks and exceptions at exit. The options are:

        * ``cleanup``: cleanup the DFK on exit without waiting for any tasks
        * ``skip``: skip all shutdown behaviour when exiting the context manager
        * ``wait``: wait for all tasks to complete when exiting normally, but exit immediately when exiting due to an exception.

        Default is ``cleanup``.
    garbage_collect : bool. optional.
        Delete task records from DFK when tasks have completed. Default: True
    internal_tasks_max_threads : int, optional
        Maximum number of threads to allocate for submit side internal tasks such as some data transfers
        or @joinapps
        Default is 10.
    monitoring : MonitoringHub, optional
        The config to use for database monitoring. Default is None which does not log to a database.
    retries : int, optional
        Set the number of retries (or available retry budget when using retry_handler) in case of failure. Default is 0.
    retry_handler : function, optional
        A user pluggable handler to decide if/how a task retry should happen.
        If no handler is specified, then each task failure incurs a retry cost
        of 1.
    run_dir : str, optional
        Path to run directory. Default is 'runinfo'.
    std_autopath : function, optional
        Sets the function used to generate stdout/stderr specifications when parsl.AUTO_LOGPATH is used. If no function
        is specified, generates paths that look like: ``rundir/NNN/task_logs/X/task_{id}_{name}{label}.{out/err}``
    strategy : str, optional
        Strategy to use for scaling blocks according to workflow needs. Can be 'simple', 'htex_auto_scale', 'none'
        or `None`.
        If 'none' or `None`, dynamic scaling will be disabled. Default is 'simple'. The literal value `None` is
        deprecated.
    strategy_period : float or int, optional
        How often the scaling strategy should be executed. Default is 5 seconds.
    max_idletime : float, optional
        The maximum idle time allowed for an executor before strategy could shut down unused blocks. Default is 120.0 seconds.
    usage_tracking : int, optional
        Set this field to 1, 2, or 3 to opt-in to Parsl's usage tracking system.
        The value represents the level of usage tracking detail to be collected.
        Setting this field to 0 will disable usage tracking. Default (this field is not set): usage tracking is not enabled.
        Parsl only collects minimal, non personally-identifiable,
        information used for reporting to our funding agencies.
    project_name: str, optional
        Option to deanonymize usage tracking data.
        If set, this value will be used as the project name in the usage tracking data and placed on the leaderboard.
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
                 executors: Optional[Iterable[ParslExecutor]] = None,
                 app_cache: bool = True,
                 checkpoint_files: Optional[Sequence[str]] = None,
                 checkpoint_mode: Union[None,
                                        Literal['task_exit'],
                                        Literal['periodic'],
                                        Literal['dfk_exit'],
                                        Literal['manual']] = None,
                 checkpoint_period: Optional[str] = None,
                 dependency_resolver: Optional[DependencyResolver] = None,
                 exit_mode: Literal['cleanup', 'skip', 'wait'] = 'cleanup',
                 garbage_collect: bool = True,
                 internal_tasks_max_threads: int = 10,
                 retries: int = 0,
                 retry_handler: Optional[Callable[[Exception, TaskRecord], float]] = None,
                 run_dir: str = 'runinfo',
                 std_autopath: Optional[Callable] = None,
                 strategy: Optional[str] = 'simple',
                 strategy_period: Union[float, int] = 5,
                 max_idletime: float = 120.0,
                 monitoring: Optional[MonitoringHub] = None,
                 usage_tracking: int = 0,
                 project_name: Optional[str] = None,
                 initialize_logging: bool = True) -> None:

        executors = tuple(executors or [])
        if not executors:
            executors = (ThreadPoolExecutor(),)

        self._executors: Sequence[ParslExecutor] = executors
        self._validate_executors()

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
        self.dependency_resolver = dependency_resolver
        self.exit_mode = exit_mode
        self.garbage_collect = garbage_collect
        self.internal_tasks_max_threads = internal_tasks_max_threads
        self.retries = retries
        self.retry_handler = retry_handler
        self.run_dir = run_dir
        self.strategy = strategy
        self.strategy_period = strategy_period
        self.max_idletime = max_idletime
        self.validate_usage_tracking(usage_tracking)
        self.usage_tracking = usage_tracking
        self.project_name = project_name
        self.initialize_logging = initialize_logging
        self.monitoring = monitoring
        self.std_autopath: Optional[Callable] = std_autopath

    @property
    def executors(self) -> Sequence[ParslExecutor]:
        return self._executors

    def _validate_executors(self) -> None:

        if len(self.executors) == 0:
            raise ConfigurationError('At least one executor must be specified')

        labels = [e.label for e in self.executors]
        duplicates = [e for n, e in enumerate(labels) if e in labels[:n]]
        if len(duplicates) > 0:
            raise ConfigurationError('Executors must have unique labels ({})'.format(
                ', '.join(['label={}'.format(repr(d)) for d in duplicates])))

    def validate_usage_tracking(self, level: int) -> None:
        if not USAGE_TRACKING_DISABLED <= level <= USAGE_TRACKING_LEVEL_3:
            raise ConfigurationError(
                f"Usage Tracking values must be 0, 1, 2, or 3 and not {level}"
            )

    def get_usage_information(self):
        return {"executors_len": len(self.executors),
                "dependency_resolver": self.dependency_resolver is not None}
