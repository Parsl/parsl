from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.monitoring import MonitoringHub


# BENC: temp class for dev purposes. should test both UDP and filesystem
# radiomodes with local executor.
class TestExecutor(ThreadPoolExecutor):
    radio_mode = "filesystem"


def fresh_config():
    executor = TestExecutor(label='threads', max_threads=4)

    # BENC: this is to check I'm overriding in subclass properly
    assert executor.radio_mode == "filesystem"

    return Config(executors=[executor],
                  monitoring=MonitoringHub(
                    hub_address="localhost",
                    hub_port=55055,
                    resource_monitoring_interval=3,
                )
         )
