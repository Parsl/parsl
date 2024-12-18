from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.monitoring import MonitoringHub

config = Config(executors=[ThreadPoolExecutor(label='threads', max_threads=4)],
                monitoring=MonitoringHub(
                    hub_address="localhost",
                    resource_monitoring_interval=3,
                )
                )
