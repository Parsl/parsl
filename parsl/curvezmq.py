import os
from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple, Union

import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator


def create_certificates(base_dir: Union[str, os.PathLike]):
    """Create server and client certificates in a private directory.

    This will overwrite existing certificate files.

    Parameters
    ----------
    base_dir : str | os.PathLike
        Parent directory of the private certificates directory.
    """
    cert_dir = os.path.join(base_dir, "certificates")
    os.makedirs(cert_dir, mode=0o700, exist_ok=True)

    zmq.auth.create_certificates(cert_dir, name="server")
    zmq.auth.create_certificates(cert_dir, name="client")

    return cert_dir


def _load_certificate(
    cert_dir: Union[str, os.PathLike], name: str
) -> Tuple[bytes, bytes]:
    if os.stat(cert_dir).st_mode & 0o777 != 0o700:
        raise OSError(f"The certificates directory must be private: {cert_dir}")

    # pyzmq creates secret key files with the '.key_secret' extension
    # Ref: https://github.com/zeromq/pyzmq/blob/ae615d4097ccfbc6b5c17de60355cbe6e00a6065/zmq/auth/certs.py#L73
    secret_key_file = os.path.join(cert_dir, f"{name}.key_secret")
    public_key, secret_key = zmq.auth.load_certificate(secret_key_file)
    if secret_key is None:
        raise ValueError(f"No secret key found in {secret_key_file}")

    return public_key, secret_key


class BaseContext(metaclass=ABCMeta):
    """Base CurveZMQ context"""

    def __init__(self, cert_dir: Optional[Union[str, os.PathLike]]) -> None:
        self.cert_dir = cert_dir
        self._ctx = zmq.Context()

    @property
    def encrypted(self):
        """Indicates whether encryption is enabled.

        False (disabled) when self.cert_dir is set to None.
        """
        return self.cert_dir is not None

    @property
    def closed(self):
        return self._ctx.closed

    @abstractmethod
    def socket(self, socket_type: int, *args, **kwargs) -> zmq.Socket:
        """Create a socket associated with this context.

        This method will apply all necessary certificates and socket options.

        Parameters
        ----------
        socket_type : int
            The socket type, which can be any of the 0MQ socket types: REQ, REP,
            PUB, SUB, PAIR, DEALER, ROUTER, PULL, PUSH, etc.

        args:
            passed to the zmq.Context.socket method.

        kwargs:
            passed to the zmq.Context.socket method.
        """
        ...

    def term(self):
        """Terminate the context."""
        self._ctx.term()

    def destroy(self, linger: Optional[int] = None):
        """Close all sockets associated with this context and then terminate
        the context.

        .. warning::

            destroy involves calling ``zmq_close()``, which is **NOT** threadsafe.
            If there are active sockets in other threads, this must not be called.

        Parameters
        ----------
        linger : int, optional
            If specified, set LINGER on sockets prior to closing them.
        """
        self._ctx.destroy(linger)

    def recreate(self, linger: Optional[int] = None):
        """Destroy then recreate the context.

        Parameters
        ----------
        linger : int, optional
            If specified, set LINGER on sockets prior to closing them.
        """
        self.destroy(linger)
        self._ctx = zmq.Context()


class ServerContext(BaseContext):
    """CurveZMQ server context

    We create server sockets via the `ctx.socket` method, which automatically
    applies the necessary certificates and socket options.

    We handle client certificate authentication in a separate dedicated thread.

    Parameters
    ----------
    cert_dir : str | os.PathLike | None
        Path to the certificate directory. Setting this to None will disable encryption.
    """

    def __init__(self, cert_dir: Optional[Union[str, os.PathLike]]) -> None:
        super().__init__(cert_dir)
        self.auth_thread = None
        if self.encrypted:
            self.auth_thread = self._start_auth_thread()

    def __del__(self):
        # Avoid issues in which the auth_thread attr was
        # previously deleted
        if getattr(self, "auth_thread", None):
            self.auth_thread.stop()

    def _start_auth_thread(self) -> ThreadAuthenticator:
        auth_thread = ThreadAuthenticator(self._ctx)
        auth_thread.start()
        # Only allow certs that are in the cert dir
        assert self.cert_dir  # For mypy
        auth_thread.configure_curve(domain="*", location=self.cert_dir)
        return auth_thread

    def socket(self, socket_type: int, *args, **kwargs) -> zmq.Socket:
        sock = self._ctx.socket(socket_type, *args, **kwargs)
        if self.encrypted:
            assert self.cert_dir  # For mypy
            _, secret_key = _load_certificate(self.cert_dir, name="server")
            try:
                # Only the clients need the server's public key to encrypt
                # messages and verify the server's identity.
                # Ref: http://curvezmq.org/page:read-the-docs
                sock.setsockopt(zmq.CURVE_SECRETKEY, secret_key)
            except zmq.ZMQError as e:
                raise ValueError("Invalid CurveZMQ key format") from e
            sock.setsockopt(zmq.CURVE_SERVER, True)  # Must come before bind

        # This flag enables IPV6 in addition to IPV4
        sock.setsockopt(zmq.IPV6, True)
        return sock

    def term(self):
        if self.auth_thread:
            self.auth_thread.stop()
        super().term()

    def destroy(self, linger: Optional[int] = None):
        if self.auth_thread:
            self.auth_thread.stop()
        super().destroy(linger)

    def recreate(self, linger: Optional[int] = None):
        super().recreate(linger)
        if self.auth_thread:
            self.auth_thread = self._start_auth_thread()


class ClientContext(BaseContext):
    """CurveZMQ client context

    We create client sockets via the `ctx.socket` method, which automatically
    applies the necessary certificates and socket options.

    Parameters
    ----------
    cert_dir : str | os.PathLike | None
        Path to the certificate directory. Setting this to None will disable encryption.
    """

    def socket(self, socket_type: int, *args, **kwargs) -> zmq.Socket:
        sock = self._ctx.socket(socket_type, *args, **kwargs)
        if self.encrypted:
            assert self.cert_dir  # For mypy
            public_key, secret_key = _load_certificate(self.cert_dir, name="client")
            server_public_key, _ = _load_certificate(self.cert_dir, name="server")
            try:
                sock.setsockopt(zmq.CURVE_PUBLICKEY, public_key)
                sock.setsockopt(zmq.CURVE_SECRETKEY, secret_key)
                sock.setsockopt(zmq.CURVE_SERVERKEY, server_public_key)
            except zmq.ZMQError as e:
                raise ValueError("Invalid CurveZMQ key format") from e
        sock.setsockopt(zmq.IPV6, True)
        return sock
