import logging
import os
import platform
import requests

logger = logging.getLogger(__name__)


def address_by_route():
    logger.debug("Finding address by querying local routing table")
    addr = os.popen("/sbin/ip route get 8.8.8.8 | awk '{print $NF;exit}'").read().strip()
    logger.debug("Address found: {}".format(addr))
    return addr


def address_by_query():
    logger.debug("Finding address by querying remote service")
    addr = requests.get('https://api.ipify.org').text
    logger.debug("Address found: {}".format(addr))
    return addr


def address_by_hostname():
    logger.debug("Finding address by using local hostname")
    addr = platform.node()
    logger.debug("Address found: {}".format(addr))
    return addr
