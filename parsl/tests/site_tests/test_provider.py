import argparse
import pytest
import parsl
import time
from parsl.app.app import python_app  # , bash_app
from parsl.providers.provider_base import JobState
from parsl.tests.site_tests.site_config_selector import fresh_config


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


@pytest.mark.local
def test_provider():
    """ Provider scaling
    """
    config = fresh_config()
    name = config.executors[0].label
    parsl.load(config)

    dfk = parsl.dfk()
    print("Trying to get executor : ", name)

    x = platform(sleep=0)
    print(x.result())

    executor = dfk.executors[name]
    provider = dfk.executors[name].provider

    # At this point we should have 1 job
    current_jobs = executor._get_job_ids()
    assert len(current_jobs) == 1, "Expected 1 job at init, got {}".format(len(current_jobs))

    status = provider.status(current_jobs)
    assert status[0].state == JobState.RUNNING, "Expected job to be in state RUNNING"

    # Scale down to 0
    scale_in_status = executor.scale_in(blocks=1)
    time.sleep(60)
    status = provider.status(scale_in_status)
    print("Block status: ", status)
    assert status[0].terminal is True, "Terminal state"
    print("Job in terminal state")

    current_jobs = executor._get_job_ids()
    assert len(current_jobs) == 0, "Expected current_jobs == 0"
    parsl.clear()
    del dfk
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="4",
                        help="Count of apps to launch")
    parser.add_argument("-t", "--time", default="60",
                        help="Sleep time for each app")

    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_provider()
