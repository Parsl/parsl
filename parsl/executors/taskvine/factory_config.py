from dataclasses import dataclass
from typing import Optional

@dataclass
class TaskVineFactoryConfig:

    # Connection and communication settings
    project_port: int = 0
    project_address: Optional[str] = None
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None
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
