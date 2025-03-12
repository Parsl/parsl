"""Script meant to be the initial program of a Flux instance."""

import argparse
import logging
import os
from os.path import dirname
from socket import gethostbyname, gethostname

import zmq


def main():
    """Run a Flux instance to completion.

    Send the path to the Flux Python package and the URI of the
    encapsulating Flux instance.
    """
    # flux imports only available when launched under Flux instance
    import flux
    import flux.job

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("protocol", help="Protocol of the parent executor's socket")
    parser.add_argument("hostname", help="hostname of the parent executor's socket")
    parser.add_argument("port", help="Port of the parent executor's socket")
    args = parser.parse_args()
    with zmq.Context() as context, context.socket(zmq.REQ) as socket:
        socket.connect(
            args.protocol + "://" + gethostbyname(args.hostname) + ":" + args.port
        )
        # send the path to the ``flux.job`` package
        socket.send(dirname(dirname(os.path.realpath(flux.__file__))).encode())
        logging.debug("Flux package path sent.")
        # collect the encapsulating Flux instance's URI
        local_uri = flux.Flux().attr_get("local-uri")
        hostname = gethostname()
        if args.hostname == hostname:
            flux_uri = local_uri
        else:
            flux_uri = "ssh://" + gethostname() + local_uri.replace("local://", "")
        logging.debug("Flux URI is %s", flux_uri)
        response = socket.recv()  # get acknowledgment
        logging.debug("Received acknowledgment %s", response)
        socket.send(flux_uri.encode())  # send URI
        logging.debug("URI sent. Blocking for response...")
        response = socket.recv()  # wait for shutdown message
        logging.debug("Response %s received, draining flux jobs...", response)
        flux.Flux().rpc("job-manager.drain").get()
        logging.debug("Flux jobs drained, exiting.")


if __name__ == "__main__":
    main()
