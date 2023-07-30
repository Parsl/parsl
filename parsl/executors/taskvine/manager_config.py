import socket
from dataclasses import dataclass
from typing import Optional

from ndcctools.taskvine.cvine import VINE_DEFAULT_PORT


@dataclass
class TaskVineManagerConfig:
    """
    Configuration of a TaskVine manager

    Parameters
    ----------

    port: int
        Network port for the manager to communicate with workers.
        A value of 0 means TaskVine chooses any available port.
        Default is VINE_DEFAULT_PORT.

    address: str
        Address of the local machine.
        Default is socket.gethostname().

    project_name: Optional[str]
        If given, TaskVine will periodically report its status and performance
        back to the global TaskVine catalog at
        http://ccl.cse.nd.edu/software/taskvine/status.
        Recommended mode of communication between manager and workers.
        Default is None. Overrides address.

    project_password_file: Optional[str]
        If given, manager and workers will authenticate each other
        with the given password file.
        Default is None.

    env_vars: Optional[dict]
        Dictionary of environment variables to set in a shell
        before executing a task.
        Default is None

    init_command: str
        Command line to run before executing a task in a worker.
        Default is ''.

    env_pack: Optional[str]
        Used to encapsulate package dependencies of tasks to
        execute them remotely without needing a shared filesystem.
        Recommended way to manage tasks' dependency requirements.
        All tasks will be executed in the encapsulated environment.
        If an absolute path to a conda environment or a conda
        environment name is given, TaskVine will package the conda
        environment in a tarball and send it along with tasks to be
        executed in a replicated conda environment.
        If a tarball of packages (*.tar.gz) is given, TaskVine
        skips the packaging step and sends the tarball along with
        tasks to be executed in a replicated conda environment.

    app_pack: bool
        Use conda-pack to prepare a self-contained Python environment
        for each app. Enabling this increases first task latency but
        does not require a common environment or a shared filesystem
        on workers.
        If env_pack is specified, app_pack if set to True will override
        env_pack.
        Default is False.

    extra_pkgs: Optional[list]
        Used when app_pack is True.
        List of extra pip/conda package names to include when packing
        the environment. Useful when app executes other (possibly
        non-Python) programs provided via pip or conda.
        Default is None.

    max_retries: int
        Set the number of retries that TaskVine will make when a task
        fails. This is distinct from Parsl level retries configured in
        parsl.config.Config. Set to None to allow TaskVine to retry tasks
        forever.
        Default is 1.

    shared_fs: bool
        Whether workers will use a shared filesystem or not. If so, TaskVine
        will not track and transfer files for execution, in exchange for
        I/O pressure on the shared filesystem.
        Default is False.

    autolabel: bool
        Use the Resource Monitor to automatically determine resource
        labels based on observed task behavior.
        Default is False.

    autolabel_window: int
        Set the number of tasks considered for autolabeling. TaskVine
        will wait for a series of N tasks with steady resource
        requirements before making a decision on labels. Increasing
        this parameter will reduce the number of failed tasks due to
        resource exhaustion when autolabeling, at the cost of increased
        resources spent collecting stats.

    autolabel_algorithm: str
        Choose an autolabeling algorithm to label resources to tasks.
        'max-xput': smartly guess a resource value, then fall back if task
        fails due to overconsumption.
        'bucketing': group tasks into buckets based on resource consumption
        similarity, and randomly choose a bucket to assign resources to the
        next task.
        'max': always allocate max resources seen so far. Allocate a whole
        machine if task fails.
        Default is max-xput.

    autolabel_window: Optional[int]
        Only apply to 'max-xput' and 'max'.
        Set the number of tasks considered for autolabeling. TaskVine
        will wait for a series of N tasks with steady resource
        requirements before making a decision on labels. Increasing
        this parameter will reduce the number of failed tasks due to
        resource exhaustion when autolabeling, at the cost of increased
        resources spent collecting stats.

    autocategory: bool
        Place each app in its own category by default. If all
        invocations of an app have similar performance characteristics,
        this will provide a reasonable set of categories automatically.
        Default is True.

    enable_peer_transfers: bool
        Whether to enable workers' capability to transfer files among
        themselves or not. Helpful if TaskVine manages all files
        tasks in the workflow use. Require that workers can communicate
        with each other via TCP ports.
        Default is True.

    wait_for_workers: Optional[int]
        Number of workers to wait for before the manager starts sending
        tasks for execution.

    vine_log_dir: Optional[str]
        Directory to store TaskVine logging facilities.
        Default is None, in which all TaskVine logs will be contained
        in the Parsl logging directory.
    """

    # Connection and communication settings
    port: int = VINE_DEFAULT_PORT
    address: str = socket.gethostname()
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None

    # Global task settings
    env_vars: Optional[dict] = None
    init_command: str = ""
    env_pack: Optional[str] = None
    app_pack: bool = False
    extra_pkgs: Optional[list] = None
    max_retries: int = 1

    # Performance-specific settings
    shared_fs: bool = False
    autolabel: bool = False
    autolabel_algorithm: str = 'max-xput'
    autolabel_window: Optional[int] = None
    autocategory: bool = True
    enable_peer_transfers: bool = True
    wait_for_workers: Optional[int] = None

    # Logging settings
    vine_log_dir: Optional[str] = None
