from dataclasses import dataclass
from typing import Optional

# Configuration encapsulation of a TaskVine manager
@dataclass
class TaskVineManagerConfig:
    
    # Connection and communication settings
    port: int = 0
    address: Optional[str] = None
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None

    # Worker settings
    env_vars: Optional[dict] = None
    init_command: Optional[str] = ''
    worker_options: Optional[str] = ''
    worker_executable: Optional[str] = 'vine_worker'

    # Factory settings
    # TODO

    # Task settings
    shared_fs: bool = False
    python_app_only: bool = False   # source
    poncho_pack_conda_by_name: Optional[str] = None
    poncho_pack_conda_by_path: Optional[str] = None
    poncho_pack: bool = False   # pack
    poncho_env: Optional[str] = None  # pack_env
    extra_pkgs: Optional[list] = None
    max_retries: int = 1

    # Performance-specific settings
    autolabel: bool = False
    autolabel_algorithm: Optional[str] = None
    autolabel_window: Optional[int] = 1
    autocategory: bool = True
    enable_peer_transfers: bool = True
    wait_for_workers: Optional[int] = 0
    full_debug: bool = False
