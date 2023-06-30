from dataclasses import dataclass
from typing import Optional
from ndcctools.taskvine.cvine import VINE_DEFAULT_PORT

# Configuration of a TaskVine manager
@dataclass
class TaskVineManagerConfig:
    
    # Connection and communication settings
    port: int = VINE_DEFAULT_PORT
    address: Optional[str] = None
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None

    # Global task settings
    init_command: str = ""
    shared_fs: bool = False
    env_vars: Optional[dict] = None # env
    env_pack: bool = False
    app_pack: bool = False
    poncho_pack_conda_by_name: Optional[str] = None
    poncho_pack_conda_by_path: Optional[str] = None
    poncho_env: Optional[str] = None  # pack_env
    poncho_run_script: str = ""
    extra_pkgs: Optional[list] = None
    max_retries: int = 1
    task_mode: str = "regular_task"  # regular_task, python_task, serverless_task
    serverless_functions: Optional[list] = None

    # Performance-specific settings
    autolabel: bool = False
    autolabel_algorithm: Optional[str] = None
    autolabel_window: Optional[int] = 1
    autocategory: bool = True
    enable_peer_transfers: bool = True
    wait_for_workers: Optional[int] = 0

    # Logging settings
    vine_log_dir: Optional[str] = None
