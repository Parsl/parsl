import pathlib
import warnings
from unittest import mock

import pytest

from parsl import curvezmq
from parsl import HighThroughputExecutor
from parsl.multiprocessing import ForkProcess

_MOCK_BASE = "parsl.executors.high_throughput.executor"


@pytest.fixture
def encrypted(request: pytest.FixtureRequest):
    if hasattr(request, "param"):
        return request.param
    return True


@pytest.fixture
def htex(encrypted: bool):
    htex = HighThroughputExecutor(encrypted=encrypted)

    yield htex

    htex.shutdown()


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
@pytest.mark.parametrize("cert_dir_provided", (True, False))
def test_htex_start_encrypted(
    encrypted: bool,
    cert_dir_provided: bool,
    htex: HighThroughputExecutor,
    tmpd_cwd: pathlib.Path,
):
    htex.run_dir = str(tmpd_cwd)
    if cert_dir_provided:
        provided_base_dir = tmpd_cwd / "provided"
        provided_base_dir.mkdir()
        cert_dir = curvezmq.create_certificates(provided_base_dir)
        htex.cert_dir = cert_dir
    else:
        cert_dir = curvezmq.create_certificates(htex.logdir)

    if not encrypted and cert_dir_provided:
        with pytest.raises(AttributeError) as pyt_e:
            htex.start()
        assert "change cert_dir to None" in str(pyt_e.value)
        return

    htex.start()

    assert htex.encrypted is encrypted
    if encrypted:
        assert htex.cert_dir == cert_dir
        assert htex.outgoing_q.zmq_context.cert_dir == cert_dir
        assert htex.incoming_q.zmq_context.cert_dir == cert_dir
        assert htex.command_client.zmq_context.cert_dir == cert_dir
        assert isinstance(htex.outgoing_q.zmq_context, curvezmq.ClientContext)
        assert isinstance(htex.incoming_q.zmq_context, curvezmq.ClientContext)
        assert isinstance(htex.command_client.zmq_context, curvezmq.ClientContext)
    else:
        assert htex.cert_dir is None
        assert htex.outgoing_q.zmq_context.cert_dir is None
        assert htex.incoming_q.zmq_context.cert_dir is None
        assert htex.command_client.zmq_context.cert_dir is None


@pytest.mark.local
@pytest.mark.parametrize("started", (True, False))
@pytest.mark.parametrize("timeout_expires", (True, False))
@mock.patch(f"{_MOCK_BASE}.logger")
def test_htex_shutdown(
    mock_logger: mock.MagicMock,
    started: bool,
    timeout_expires: bool,
    htex: HighThroughputExecutor,
):
    mock_ix_proc = mock.Mock(spec=ForkProcess)

    if started:
        htex.interchange_proc = mock_ix_proc
        mock_ix_proc.is_alive.return_value = True

    if not timeout_expires:
        # Simulate termination of the Interchange process
        def kill_interchange(*args, **kwargs):
            mock_ix_proc.is_alive.return_value = False

        mock_ix_proc.terminate.side_effect = kill_interchange

    htex.shutdown()

    mock_logs = mock_logger.info.call_args_list
    if started:
        assert mock_ix_proc.terminate.called
        assert mock_ix_proc.join.called
        assert {"timeout": 10} == mock_ix_proc.join.call_args[1]
        if timeout_expires:
            assert "Unable to terminate Interchange" in mock_logs[1][0][0]
            assert mock_ix_proc.kill.called
        assert "Attempting" in mock_logs[0][0][0]
        assert "Finished" in mock_logs[-1][0][0]
    else:
        assert not mock_ix_proc.terminate.called
        assert not mock_ix_proc.join.called
        assert "has not started" in mock_logs[0][0][0]


@pytest.mark.local
def test_max_workers_per_node():
    with pytest.warns(DeprecationWarning) as record:
        htex = HighThroughputExecutor(max_workers_per_node=1, max_workers=2)

    warning_msg = "max_workers is deprecated"
    assert any(warning_msg in str(warning.message) for warning in record)

    # Ensure max_workers_per_node takes precedence
    assert htex.max_workers_per_node == htex.max_workers == 1
