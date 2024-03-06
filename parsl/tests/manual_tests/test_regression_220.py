import time

import pytest

from parsl import DataFlowKernel, set_stream_logger
from parsl.tests.configs.local_threads import config


@pytest.mark.skip('meant to be tested by hand')
def test_220():
    """Test async usage_tracking behavior for issue #220 """

    print("This test assumes /etc/resolv being misconfigured")
    with open("/etc/resolv.conf", 'r') as f:
        for line in f.readlines():
            line = line.strip()
            print("Line: [{}]".format(line))
            if line.startswith("nameserver") and line != "nameserver 2.2.2.2":
                assert False, "/etc/resolv.conf should be misconfigured"

    start = time.time()
    set_stream_logger()
    dfk = DataFlowKernel(config=config)
    delta = time.time() - start
    print("Time taken : ", delta)
    assert delta < 1, "DFK took too much time to start, delta:{}".format(delta)
    dfk.cleanup()


if __name__ == "__main__":

    test_220()
