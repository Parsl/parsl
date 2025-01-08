from dataclasses import dataclass
from typing import Optional


@dataclass
class TaskVineFactoryConfig:
    """
    Configuration of a TaskVine factory.
    Default is to spawn one local worker.

    Parameters
    ----------
    factory_timeout: int
        Exit if no manager seen in $factory_timeout seconds.
        Kill all workers on upon exit.

    scratch_dir: Optional[str]
        Directory for factory to operate.
        Default is None, which TaskVine factory will use a directory
        residing inside Parsl logging directory.

    min_workers: int
        Number of minimum workers connecting to manager at all times.
        Default is 1.

    max_workers: int
        Number of maximum workers connecting to manager at all times.
        Default is 1.

    workers_per_cycle: int
        Maximum number of new workers per 30s.

    worker_options: Optional[str]
        Additional options to pass to workers. Run
        ``vine_worker --help`` for more details.
        Default is None.

    worker_executable: str
        Executable to run workers.
        Default is 'vine_worker', which finds the executable
        on $PATH.

    worker_timeout: int
        Number of seconds for workers to wait for manager's contact
        before exiting.
        Default is 300.

    cores: Optional[int]
        Number of cores a worker should have.
        Default is None, which allows a worker to use all available
        cores of the machine it lands on.

    gpus: Optional[int]
        Number of gpus a worker should have.
        Default is None, which allows a worker to use all available
        gpus of the machine it lands on.

    memory: Optional[int]
        Amount of memory in MBs a worker should have.
        Default is None, which allows a worker to use all available
        memory of the machine it lands on.

    disk: Optional[int]
        Amount of disk in MBs a worker should have.
        Default is None, which allows a worker to use all available
        disk space of the machine it lands on.

    python_env: Optional[str]
        Run workers inside this python environment tarball.
        Default is None.

    batch_type: str
        Batch systems the factory should submit workers as jobs.
        'local': spawn local workers.
        'condor': Submit workers through HTCondor batch system.
        'sge': Submit workers through SGE batch system.
        'slurm': Submit workers through SLURM batch system.
        Default is 'local'.

    condor_requirements: Optional[str]
        String describing the condor requirements for slots
        that workers should land on.
        Default is None.

    batch_options: Optional[str]
        String describing generic batch options. Varies with the
        type of batch system.
        Default is None.
    """

    # Connection and communication settings
    factory_timeout: int = 300

    # Logging settings
    scratch_dir: Optional[str] = None

    # Scale settings
    min_workers: int = 1
    max_workers: int = 1
    workers_per_cycle: int = 1

    # Worker settings
    worker_options: Optional[str] = None
    worker_executable: str = 'vine_worker'
    worker_timeout: int = 300
    cores: Optional[int] = None
    gpus: Optional[int] = None
    memory: Optional[int] = None
    disk: Optional[int] = None
    python_env: Optional[str] = None

    # Batch settings
    batch_type: str = "local"
    condor_requirements: Optional[str] = None
    batch_options: Optional[str] = None

    # Private attributes, users should not touch
    _project_port: int = 0
    _project_address: Optional[str] = None
    _project_name: Optional[str] = None
    _project_password_file: Optional[str] = None
