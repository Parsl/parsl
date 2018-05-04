from parsl import DataFlowKernel, set_stream_logger
import socket
import time
from parsl.configs.local import localThreads as config


def internet_on(host="8.8.8.8", port=53, timeout=3):
    """
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as ex:
        return False


def test_220():
    print("This test should be run only with no internet connectivity")
    x = internet_on()
    assert x is False, "Internet is available test fails"

    start = time.time()
    set_stream_logger()
    dfk = DataFlowKernel(config=config)
    delta = time.time() - start
    assert delta < 1, "DFK took too much time to start, delta:{}".format(delta)
    dfk.cleanup()


if __name__ == "__main__":

    test_220()
