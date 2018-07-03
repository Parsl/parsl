from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        ThreadPoolExecutor(max_threads=4),
    ],
    app_cache=False,
    run_dir=get_rundir()
)
