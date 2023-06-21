from dataclasses import dataclass
from typing import Optional

@dataclass
class TaskVineFactoryConfig:
    init_command: Optional[str] = ''
    worker_options: Optional[str] = ''
    worker_executable: Optional[str] = 'vine_worker'
