import pathlib

import pytest

from parsl import curvezmq
from parsl import HighThroughputExecutor


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False))
@pytest.mark.parametrize("cert_dir_provided", (True, False))
def test_htex_start_encrypted(
    encrypted: bool, cert_dir_provided: bool, tmpd_cwd: pathlib.Path
):
    htex = HighThroughputExecutor(encrypted=encrypted)
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
