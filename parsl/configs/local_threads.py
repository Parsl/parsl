from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[ThreadPoolExecutor()],
    usage_tracking=LEVEL_1,
)
