from dataclasses import dataclass
from typing import Optional

@dataclass
class TaskVineManagerConfig:
    port: int = 0
    project_name: Optional[str] = None
    project_password_file: Optional[str] = None
    address: Optional[str] = None
    env: Optional[dict] = None
    shared_fs: bool = False
    python_app_only: bool = False   # source
    poncho_pack_conda_by_name: Optional[str] = None
    poncho_pack_conda_by_path: Optional[str] = None
    poncho_pack: bool = False   # pack
    poncho_env: Optional[str] = None  # pack_env
    extra_pkgs: Optional[list]

