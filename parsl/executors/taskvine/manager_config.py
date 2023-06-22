from dataclasses import dataclass
from typing import Optional

# Configuration of a TaskVine manager
@dataclass
class TaskVineManagerConfig:
    
    # Connection and communication settings
    port: int = 0
    address: Optional[str] = None
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None

    # Task settings
    shared_fs: bool = False
    env_vars: Optional[dict] = None # env
    env_pack: bool = False
    app_pack: bool = False
    poncho_pack_conda_by_name: Optional[str] = None
    poncho_pack_conda_by_path: Optional[str] = None
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
