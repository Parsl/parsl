from dataclasses import dataclass
from typing import Optional

@dataclass
class TaskVineFactoryConfig:

    # Connection and communication settings
    port: int = 0
    address: Optional[str] = None
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None

    # Worker settings
    worker_options: Optional[str] = ''
    worker_executable: Optional[str] = 'vine_worker'
