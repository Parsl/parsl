import os
import pathlib
from typing import Union
from unittest import mock

import pytest
import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

from parsl import curvezmq

ADDR = "tcp://127.0.0.1"


def get_server_socket(ctx: curvezmq.ServerContext):
    sock = ctx.socket(zmq.PULL)
    sock.setsockopt(zmq.RCVTIMEO, 200)
    sock.setsockopt(zmq.LINGER, 0)
    port = sock.bind_to_random_port(ADDR)
    return sock, port


def get_client_socket(ctx: curvezmq.ClientContext, port: int):
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.SNDTIMEO, 200)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"{ADDR}:{port}")
    return sock


def get_external_server_socket(
    ctx: Union[curvezmq.ServerContext, zmq.Context], secret_key: bytes
):
    sock = ctx.socket(zmq.PULL)
    sock.setsockopt(zmq.RCVTIMEO, 200)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.CURVE_SECRETKEY, secret_key)
    sock.setsockopt(zmq.CURVE_SERVER, True)
    port = sock.bind_to_random_port(ADDR)
    return sock, port


def get_external_client_socket(
    ctx: Union[curvezmq.ClientContext, zmq.Context],
    public_key: bytes,
    secret_key: bytes,
    server_key: bytes,
    port: int,
):
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.CURVE_PUBLICKEY, public_key)
    sock.setsockopt(zmq.CURVE_SECRETKEY, secret_key)
    sock.setsockopt(zmq.CURVE_SERVERKEY, server_key)
    sock.connect(f"{ADDR}:{port}")
    return sock


@pytest.fixture
def encrypted(request: pytest.FixtureRequest):
    if hasattr(request, "param"):
        return request.param
    return True


@pytest.fixture
def cert_dir(encrypted: bool, tmpd_cwd: pathlib.Path):
    if not encrypted:
        return None
    return curvezmq.create_certificates(tmpd_cwd)


@pytest.fixture
def server_ctx(cert_dir: Union[str, None]):
    ctx = curvezmq.ServerContext(cert_dir)
    yield ctx
    ctx.destroy()


@pytest.fixture
def client_ctx(cert_dir: Union[str, None]):
    ctx = curvezmq.ClientContext(cert_dir)
    yield ctx
    ctx.destroy()


@pytest.fixture
def zmq_ctx():
    ctx = zmq.Context()
    yield ctx
    ctx.destroy()


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_client_context_init(cert_dir: Union[str, None]):
    ctx = curvezmq.ClientContext(cert_dir=cert_dir)

    assert ctx.cert_dir == cert_dir
    if cert_dir is None:
        assert not ctx.encrypted
    else:
        assert ctx.encrypted

    ctx.destroy()


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_server_context_init(cert_dir: Union[str, None]):
    ctx = curvezmq.ServerContext(cert_dir=cert_dir)

    assert ctx.cert_dir == cert_dir
    if cert_dir is None:
        assert not ctx.encrypted
        assert not ctx.auth_thread
    else:
        assert ctx.encrypted
        assert isinstance(ctx.auth_thread, ThreadAuthenticator)

    ctx.destroy()


@pytest.mark.local
def test_create_certificates(tmpd_cwd: pathlib.Path):
    cert_dir = tmpd_cwd / "certificates"
    assert not os.path.exists(cert_dir)

    ret = curvezmq.create_certificates(tmpd_cwd)

    assert str(cert_dir) == ret
    assert os.path.exists(cert_dir)
    assert os.stat(cert_dir).st_mode & 0o777 == 0o700
    assert len(os.listdir(cert_dir)) == 4


@pytest.mark.local
def test_create_certificates_overwrite(tmpd_cwd: pathlib.Path):
    cert_dir = curvezmq.create_certificates(tmpd_cwd)
    client_pub_1, client_sec_1 = curvezmq._load_certificate(cert_dir, name="client")
    server_pub_1, server_sec_1 = curvezmq._load_certificate(cert_dir, name="server")

    curvezmq.create_certificates(tmpd_cwd)
    client_pub_2, client_sec_2 = curvezmq._load_certificate(cert_dir, name="client")
    server_pub_2, server_sec_2 = curvezmq._load_certificate(cert_dir, name="server")

    assert client_pub_1 != client_pub_2
    assert client_sec_1 != client_sec_2
    assert server_pub_1 != server_pub_2
    assert server_sec_1 != server_sec_2


@pytest.mark.local
def test_cert_dir_not_private(tmpd_cwd: pathlib.Path):
    cert_dir = tmpd_cwd / "certificates"
    os.makedirs(cert_dir, mode=0o777)
    client_ctx = curvezmq.ClientContext(cert_dir)
    server_ctx = curvezmq.ServerContext(cert_dir)

    err_msg = "directory must be private"

    with pytest.raises(OSError) as pyt_e:
        client_ctx.socket(zmq.REQ)
    assert err_msg in str(pyt_e.value)

    with pytest.raises(OSError) as pyt_e:
        server_ctx.socket(zmq.REP)
    assert err_msg in str(pyt_e.value)

    client_ctx.destroy()
    server_ctx.destroy()


@pytest.mark.local
def test_missing_cert_dir():
    cert_dir = "/bad/cert/dir"
    client_ctx = curvezmq.ClientContext(cert_dir)
    server_ctx = curvezmq.ServerContext(cert_dir)

    err_msg = "No such file or directory"

    with pytest.raises(FileNotFoundError) as pyt_e:
        client_ctx.socket(zmq.REQ)
    assert err_msg in str(pyt_e.value)

    with pytest.raises(FileNotFoundError) as pyt_e:
        server_ctx.socket(zmq.REP)
    assert err_msg in str(pyt_e.value)

    client_ctx.destroy()
    server_ctx.destroy()


@pytest.mark.local
def test_missing_secret_file(tmpd_cwd: pathlib.Path):
    cert_dir = tmpd_cwd / "certificates"
    os.makedirs(cert_dir, mode=0o700)

    client_ctx = curvezmq.ClientContext(cert_dir)
    server_ctx = curvezmq.ServerContext(cert_dir)

    err_msg = "Invalid certificate file"

    with pytest.raises(OSError) as pyt_e:
        client_ctx.socket(zmq.REQ)
    assert err_msg in str(pyt_e.value)

    with pytest.raises(OSError) as pyt_e:
        server_ctx.socket(zmq.REP)
    assert err_msg in str(pyt_e.value)

    client_ctx.destroy()
    server_ctx.destroy()


@pytest.mark.local
def test_bad_secret_file(tmpd_cwd: pathlib.Path):
    client_sec = tmpd_cwd / "client.key_secret"
    server_sec = tmpd_cwd / "server.key_secret"
    client_sec.write_text("bad")
    server_sec.write_text("boy")

    client_ctx = curvezmq.ClientContext(tmpd_cwd)
    server_ctx = curvezmq.ServerContext(tmpd_cwd)

    err_msg = "No public key found"

    with pytest.raises(ValueError) as pyt_e:
        client_ctx.socket(zmq.REQ)
    assert err_msg in str(pyt_e.value)

    with pytest.raises(ValueError) as pyt_e:
        server_ctx.socket(zmq.REP)
    assert err_msg in str(pyt_e.value)

    client_ctx.destroy()
    server_ctx.destroy()


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_client_context_term(client_ctx: curvezmq.ClientContext):
    assert not client_ctx.closed

    client_ctx.term()

    assert client_ctx.closed


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_server_context_term(server_ctx: curvezmq.ServerContext, encrypted: bool):
    assert not server_ctx.closed
    if encrypted:
        assert server_ctx.auth_thread
        assert server_ctx.auth_thread.pipe

    server_ctx.term()

    assert server_ctx.closed
    if encrypted:
        assert server_ctx.auth_thread
        assert not server_ctx.auth_thread.pipe


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_client_context_destroy(client_ctx: curvezmq.ClientContext):
    sock = client_ctx.socket(zmq.REP)

    assert not client_ctx.closed

    client_ctx.destroy()

    assert sock.closed
    assert client_ctx.closed


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_server_context_destroy(server_ctx: curvezmq.ServerContext, encrypted: bool):
    sock = server_ctx.socket(zmq.REP)

    assert not server_ctx.closed
    if encrypted:
        assert server_ctx.auth_thread
        assert server_ctx.auth_thread.pipe

    server_ctx.destroy()

    assert sock.closed
    assert server_ctx.closed
    if encrypted:
        assert server_ctx.auth_thread
        assert not server_ctx.auth_thread.pipe


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_client_context_recreate(client_ctx: curvezmq.ClientContext):
    hidden_ctx = client_ctx._ctx
    sock = client_ctx.socket(zmq.REQ)

    assert not sock.closed
    assert not client_ctx.closed

    client_ctx.recreate()

    assert sock.closed
    assert not client_ctx.closed
    assert hidden_ctx != client_ctx._ctx
    assert hidden_ctx.closed


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_server_context_recreate(server_ctx: curvezmq.ServerContext, encrypted: bool):
    hidden_ctx = server_ctx._ctx
    sock = server_ctx.socket(zmq.REP)

    assert not sock.closed
    assert not server_ctx.closed
    if encrypted:
        assert server_ctx.auth_thread
        auth_thread = server_ctx.auth_thread
        assert auth_thread.pipe

    server_ctx.recreate()

    assert sock.closed
    assert not server_ctx.closed
    assert hidden_ctx.closed
    assert hidden_ctx != server_ctx._ctx
    if encrypted:
        assert server_ctx.auth_thread
        assert auth_thread != server_ctx.auth_thread
        assert server_ctx.auth_thread.pipe


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_connection(
    server_ctx: curvezmq.ServerContext, client_ctx: curvezmq.ClientContext
):
    server_socket, port = get_server_socket(server_ctx)
    client_socket = get_client_socket(client_ctx, port)

    msg = b"howdy"
    client_socket.send(msg)
    recv = server_socket.recv()

    assert recv == msg


@pytest.mark.local
@mock.patch.object(curvezmq, "_load_certificate")
def test_invalid_key_format(
    mock_load_cert: mock.MagicMock,
    server_ctx: curvezmq.ServerContext,
    client_ctx: curvezmq.ClientContext,
):
    mock_load_cert.return_value = (b"badkey", b"badkey")

    with pytest.raises(ValueError) as e1_info:
        server_ctx.socket(zmq.REP)
    with pytest.raises(ValueError) as e2_info:
        client_ctx.socket(zmq.REQ)
    e1, e2 = e1_info.exconly, e2_info.exconly

    assert str(e1) == str(e2)
    assert "Invalid CurveZMQ key format" in str(e1)


@pytest.mark.local
def test_invalid_client_keys(server_ctx: curvezmq.ServerContext, zmq_ctx: zmq.Context):
    server_socket, port = get_server_socket(server_ctx)

    cert_dir = server_ctx.cert_dir
    assert cert_dir  # For mypy
    public_key, secret_key = curvezmq._load_certificate(cert_dir, "client")
    server_key, _ = curvezmq._load_certificate(cert_dir, "server")

    BAD_PUB_KEY, BAD_SEC_KEY = zmq.curve_keypair()
    msg = b"howdy"

    client_socket = get_external_client_socket(
        zmq_ctx,
        public_key,
        secret_key,
        server_key,
        port,
    )
    client_socket.send(msg)
    assert server_socket.recv() == msg

    client_socket = get_external_client_socket(
        zmq_ctx,
        BAD_PUB_KEY,
        BAD_SEC_KEY,
        server_key,
        port,
    )
    client_socket.send(msg)
    with pytest.raises(zmq.Again):
        server_socket.recv()

    client_socket = get_external_client_socket(
        zmq_ctx,
        public_key,
        secret_key,
        BAD_PUB_KEY,
        port,
    )
    client_socket.send(msg)
    with pytest.raises(zmq.Again):
        server_socket.recv()

    # Ensure sockets are operational
    client_socket = get_external_client_socket(
        zmq_ctx,
        public_key,
        secret_key,
        server_key,
        port,
    )
    client_socket.send(msg)
    assert server_socket.recv() == msg


@pytest.mark.local
def test_invalid_server_key(client_ctx: curvezmq.ClientContext, zmq_ctx: zmq.Context):
    cert_dir = client_ctx.cert_dir
    assert cert_dir  # For mypy
    _, secret_key = curvezmq._load_certificate(cert_dir, "server")

    _, BAD_SEC_KEY = zmq.curve_keypair()
    msg = b"howdy"

    server_socket, port = get_external_server_socket(zmq_ctx, secret_key)
    client_socket = get_client_socket(client_ctx, port)
    client_socket.send(msg)
    assert server_socket.recv() == msg

    server_socket, port = get_external_server_socket(zmq_ctx, BAD_SEC_KEY)
    client_socket = get_client_socket(client_ctx, port)
    client_socket.send(msg)
    with pytest.raises(zmq.Again):
        server_socket.recv()

    # Ensure sockets are operational
    server_socket, port = get_external_server_socket(zmq_ctx, secret_key)
    client_socket = get_client_socket(client_ctx, port)
    client_socket.send(msg)
    assert server_socket.recv() == msg
