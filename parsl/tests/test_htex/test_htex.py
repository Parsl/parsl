import logging
import pathlib
from subprocess import Popen, TimeoutExpired
from typing import Optional, Sequence
from unittest import mock

import pytest

from parsl import HighThroughputExecutor, curvezmq

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
def test_htex_shutdown(
    started: bool,
    timeout_expires: bool,
    htex: HighThroughputExecutor,
    caplog
):
    mock_ix_proc = mock.Mock(spec=Popen)

    if started:
        htex.interchange_proc = mock_ix_proc

    # This will, in the absence of any exit trigger, block forever if
    # no timeout is given and if the interchange does not terminate.
    # Raise an exception to report that, rather than actually block,
    # and hope that nothing is catching that exception.

    # this function implements the behaviour if the interchange has
    # not received a termination call
    def proc_wait_alive(timeout):
        if timeout:
            raise TimeoutExpired(cmd="mock-interchange", timeout=timeout)
        else:
            raise RuntimeError("This wait call would hang forever")

    def proc_wait_terminated(timeout):
        return 0

    mock_ix_proc.wait.side_effect = proc_wait_alive

    if not timeout_expires:
        # Simulate termination of the Interchange process
        def kill_interchange(*args, **kwargs):
            mock_ix_proc.wait.side_effect = proc_wait_terminated

        mock_ix_proc.terminate.side_effect = kill_interchange

    with caplog.at_level(logging.INFO):
        htex.shutdown()

    if started:
        assert mock_ix_proc.terminate.called
        assert mock_ix_proc.wait.called
        assert {"timeout": 10} == mock_ix_proc.wait.call_args[1]
        if timeout_expires:
            assert "Unable to terminate Interchange" in caplog.text
            assert mock_ix_proc.kill.called
        assert "Attempting HighThroughputExecutor shutdown" in caplog.text
        assert "Finished HighThroughputExecutor shutdown" in caplog.text
    else:
        assert not mock_ix_proc.terminate.called
        assert not mock_ix_proc.wait.called
        assert "HighThroughputExecutor has not started" in caplog.text


@pytest.mark.local
@pytest.mark.parametrize("cmd", (None, "custom-launch-cmd"))
def test_htex_worker_pool_launch_cmd(cmd: Optional[str]):
    if cmd:
        htex = HighThroughputExecutor(launch_cmd=cmd)
        assert htex.launch_cmd == cmd
    else:
        htex = HighThroughputExecutor()
        assert htex.launch_cmd.startswith("process_worker_pool.py")


@pytest.mark.local
@pytest.mark.parametrize("cmd", (None, ["custom", "launch", "cmd"]))
def test_htex_interchange_launch_cmd(cmd: Optional[Sequence[str]]):
    if cmd:
        htex = HighThroughputExecutor(interchange_launch_cmd=cmd)
        assert htex.interchange_launch_cmd == cmd
    else:
        htex = HighThroughputExecutor()
        assert htex.interchange_launch_cmd == ["interchange.py"]
