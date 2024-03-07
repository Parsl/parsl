import logging
import os
import parsl
import pytest
import threading
import time

from diaspora_event_sdk import KafkaConsumer
from diaspora_event_sdk import Client as GlobusClient


logger = logging.getLogger(__name__)


def consumer_check(consumer):
    start = time.time()
    for record in consumer:
        end = time.time()
        if end - start > 60:
            assert False, "No messages received"
        if record:
            break


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    import time
    time.sleep(3)

    return 5


@pytest.mark.skip(reason="requires diaspora login")
def test_diaspora_radio():
    c = GlobusClient()
    topic = "radio-test" + c.subject_openid[-12:]
    consumer = KafkaConsumer(topic)
    # open a new thread for the consumer
    consumer_thread = threading.Thread(target=consumer_check, args=(consumer,))
    consumer_thread.start()

    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    c = fresh_config()
    c.executors[0].radio_mode = "diaspora"
    parsl.load(c)

    logger.info("invoking and waiting for result")
    assert this_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    consumer_thread.join()

    logger.info("all done")
