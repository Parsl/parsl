import pytest

from parsl.addresses import tcp_url


@pytest.mark.local
@pytest.mark.parametrize("address, port,expected", [
    ("127.0.0.1", 55001, "tcp://127.0.0.1:55001"),
    ("127.0.0.1", "55001", "tcp://127.0.0.1:55001"),
    ("127.0.0.1", None, "tcp://127.0.0.1"),
    ("::1", "55001", "tcp://[::1]:55001"),
    ("::ffff:127.0.0.1", 55001, "tcp://[::ffff:127.0.0.1]:55001"),
    ("::ffff:127.0.0.1", None, "tcp://::ffff:127.0.0.1"),
    ("::ffff:127.0.0.1", None, "tcp://::ffff:127.0.0.1"),
    ("*", None, "tcp://*"),
])
def test_tcp_url(address, port, expected):
    """Confirm valid address generation"""
    result = tcp_url(address, port)
    assert result == expected
