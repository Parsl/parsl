import parsl
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

local_threads = Config(
  executors=[ThreadPoolExecutor(max_threads=4)]
)
