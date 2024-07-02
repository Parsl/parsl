from unittest.mock import Mock

import pytest

import parsl
from parsl.channels.base import Channel
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider


@pytest.mark.local
def test_dfk_close():

    mock_channel = Mock(spec=Channel)

    # block settings all 0 because the mock channel won't be able to
    # do anything to make a block exist
    p = LocalProvider(channel=mock_channel, init_blocks=0, min_blocks=0, max_blocks=0)

    e = HighThroughputExecutor(provider=p)

    c = parsl.Config(executors=[e])
    with parsl.load(c):
        pass

    assert mock_channel.close.called
