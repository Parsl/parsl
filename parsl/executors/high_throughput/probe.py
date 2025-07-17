import logging
import pickle
from contextlib import ExitStack
from typing import Optional

import zmq

from parsl import curvezmq

logger = logging.getLogger(__name__)


def probe_addresses(
    zmq_context: curvezmq.ClientContext,
    addresses: str,
    timeout_ms: int = 120_000,
    identity: Optional[bytes] = None,
):
    """
    Given a single-line CSV list of addresses, return the first proven valid address.

    This function will connect to each address in ``addresses`` (a comma-separated
    list of URLs) and attempt to send a CONNECTION_PROBE packet.  Returns the first
    address that receives a response.

    If no address receives a response within the ``timeout_ms`` (specified in
    milliseconds), then raise ``ConnectionError``.

    :param zmq_context: A ZMQ Context; the call-site may provide an encrypted ZMQ
        context for assurance that the returned address is the expected and correct
        endpoint
    :param addresses: a comma-separated string of addresses to attempt.  Example:
        ``tcp://127.0.0.1:1234,tcp://[3812::03aa]:5678``
    :param timeout_ms: how long to wait for a response from the probes.  The probes
        are initiated and await concurrently, so this timeout will be the total wall
        time in the worst case of "no addresses are valid."
    :param identity: a ZMQ connection identity; used for logging connection probes
        at the interchange
    :raises: ``ConnectionError`` if no addresses are determined valid
    :returns: a single address, the first one that received a response
    """
    if not addresses:
        raise ValueError("No address to probe!")

    sock_map = {}
    urls = addresses.split(",")
    with ExitStack() as stk:
        poller = zmq.Poller()
        for url in urls:
            logger.debug("Testing ZMQ connection to url: %s", url)
            s: zmq.Socket = stk.enter_context(zmq_context.socket(zmq.DEALER))
            s.setsockopt(zmq.LINGER, 0)
            s.setsockopt(zmq.IPV6, True)
            if identity:
                s.setsockopt(zmq.IDENTITY, identity)
            stk.enter_context(s.connect(url))
            poller.register(s, zmq.POLLIN)
            sock_map[s] = url

            s.send(pickle.dumps({'type': 'connection_probe'}))

        for sock, evt in poller.poll(timeout=timeout_ms):
            sock.recv()  # clear the buffer for good netizenry
            return sock_map.get(sock)

    addys = ", ".join(urls)  # just slightly more human friendly
    raise ConnectionError(f"No viable ZMQ url from: {addys}")


class TestWorker:

    def __init__(self, addresses, port):
        uid = str(uuid.uuid4())
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)

        address = probe_addresses(addresses, port)
        print("Viable address :", address)
        self.task_incoming.connect(tcp_url(address, port))

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        HEARTBEAT_CODE = (2 ** 32) - 1
        heartbeat = (HEARTBEAT_CODE).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        print("Return from heartbeat: {}".format(r))


if __name__ == "__main__":
    import argparse
    import uuid

    from parsl.addresses import get_all_addresses, tcp_url

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", required=True,
                        help="Port to connect to")

    args = parser.parse_args()
    addresses = get_all_addresses()
    worker = TestWorker(addresses, args.port)
    worker.heartbeat()
