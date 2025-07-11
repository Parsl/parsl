import logging
import os
import socket
import time

import pytest
import zmq

import parsl

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    return 5


@pytest.mark.local
def test_fuzz():
    """This test sends fuzz into the ZMQ radio receiver that HTEX starts
    for receiving monitoring messages from the interchange, and checks
    that monitoring still records things ok.
    """

    import sqlalchemy
    from sqlalchemy import text

    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    logger.info("invoking apps and waiting for result")

    assert this_app().result() == 5
    assert this_app().result() == 5

    # now we've run some apps, send fuzz into the monitoring ZMQ
    # socket, before trying to run some more tests.

    # there are different kinds of fuzz:
    # could send ZMQ messages that are weird
    # could send random bytes to the TCP socket
    #   the latter is what i'm most suspicious of in my present investigation

    # dig out the interchange port...
    hub_address = parsl.dfk().executors["htex_Local"].loopback_address
    hub_zmq_port = parsl.dfk().executors["htex_Local"].hub_zmq_port

    # this will send a string to a new socket connection
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((hub_address, hub_zmq_port))
        s.sendall(b'fuzzing\r')

    context = zmq.Context()
    channel_timeout = 10000  # in milliseconds
    hub_channel = context.socket(zmq.DEALER)
    hub_channel.setsockopt(zmq.LINGER, 0)
    hub_channel.set_hwm(0)
    hub_channel.setsockopt(zmq.SNDTIMEO, channel_timeout)
    hub_channel.connect("tcp://{}:{}".format(hub_address, hub_zmq_port))

    # this will send a non-object down the DFK's existing ZMQ connection
    hub_channel.send(b'FuzzyByte\rSTREAM')

    # This following attack is commented out, because monitoring is not resilient
    # to this.
    # In practice, it works some of the time but in some circumstances,
    # it would still abandon writing multiple unrelated records to the database,
    # causing ongoing monitoring data loss.

    # This will send an unusual python object down the
    # DFK's existing ZMQ connection. this doesn't break the router,
    # but breaks the db_manager in a way that isn't reported until
    # the very end of the run, and database writing is abandoned
    # rather than completing, in this case.
    # I'm unclear if this is a case we should be trying to handle.
    # parsl.dfk().monitoring._dfk_channel.send_pyobj("FUZZ3")

    # hopefully long enough for any breakage to happen
    # before attempting to run more tasks
    time.sleep(5)

    assert this_app().result() == 5
    assert this_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        result = connection.execute(text("SELECT COUNT(*) FROM workflow"))
        (c, ) = result.first()
        assert c == 1

        result = connection.execute(text("SELECT COUNT(*) FROM task"))
        (c, ) = result.first()
        assert c == 4

        result = connection.execute(text("SELECT COUNT(*) FROM try"))
        (c, ) = result.first()
        assert c == 4

    logger.info("all done")
