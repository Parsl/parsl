"""This module contains several helper functions which can be used to
find an address of the submitting system, for example to use as the
address parameter for HighThroughputExecutor.

The helper to use depends on the network environment around the submitter,
so some experimentation will probably be needed to choose the correct one.
"""

import ipaddress
import logging
import platform
import socket

import requests

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]
import struct
from typing import Callable, List, Set, Union

import psutil
import typeguard

logger = logging.getLogger(__name__)


def address_by_route() -> str:
    """Finds an address for the local host by querying the local routing table
       for the route to Google DNS.

       This will return an unusable value when the internet-facing address is
       not reachable from workers.
    """
    logger.debug("Finding address by querying local routing table")

    # original author unknown
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    addr = s.getsockname()[0]
    s.close()
    logger.debug("Address found: {}".format(addr))
    return addr


@typeguard.typechecked
def address_by_query(timeout: float = 30) -> str:
    """Finds an address for the local host by querying ipify. This may
       return an unusable value when the host is behind NAT, or when the
       internet-facing address is not reachable from workers.
       Parameters:
       -----------

       timeout : float
          Timeout for the request in seconds. Default: 30s
    """
    logger.debug("Finding address by querying remote service")
    response = requests.get('https://api.ipify.org', timeout=timeout)

    if response.status_code == 200:
        addr = response.text
        logger.debug("Address found: {}".format(addr))
        return addr
    else:
        raise RuntimeError("Remote service returned unexpected HTTP status code {}".format(response.status_code))


def address_by_hostname() -> str:
    """Returns the hostname of the local host.

       This will return an unusable value when the hostname cannot be
       resolved from workers.
    """
    logger.debug("Finding address by using local hostname")
    addr = platform.node()
    ip_addr = socket.gethostbyname(addr)
    logger.debug("Address found: {}".format(ip_addr))
    return ip_addr


@typeguard.typechecked
def address_by_interface(ifname: str) -> str:
    """Returns the IP address of the given interface name, e.g. 'eth0'

    This is taken from a Stack Overflow answer:
    https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-of-eth0-in-python#24196955


    Parameters
    ----------
    ifname : str
        Name of the interface whose address is to be returned. Required.

    """
    assert fcntl is not None, "This function is not supported on your OS."
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
    )[20:24])


def get_all_addresses() -> Set[str]:
    """ Uses a combination of methods to determine possible addresses.

    Returns:
         list of addresses as strings
    """
    net_interfaces = psutil.net_if_addrs()

    s_addresses = set()
    for interface in net_interfaces:
        try:
            s_addresses.add(address_by_interface(interface))
        except Exception:
            logger.debug("Ignoring failure to fetch address from interface {}".format(interface))

    resolution_functions: List[Callable[[], str]]
    resolution_functions = [address_by_hostname, address_by_route, address_by_query]
    for f in resolution_functions:
        try:
            s_addresses.add(f())
        except Exception:
            logger.debug("Ignoring an address finder exception")

    return s_addresses


def get_any_address() -> str:
    """ Uses a combination of methods to find any address of the local machine.

    Returns:
        one address in string
    """
    net_interfaces = psutil.net_if_addrs()

    addr = ''
    for interface in net_interfaces:
        try:
            addr = address_by_interface(interface)
            return addr
        except Exception:
            logger.info("Ignoring failure to fetch address from interface {}".format(interface))

    resolution_functions: List[Callable[[], str]]
    resolution_functions = [address_by_hostname, address_by_route, address_by_query]
    for f in resolution_functions:
        try:
            addr = f()
            return addr
        except Exception:
            logger.info("Ignoring an address finder exception")

    if addr == '':
        raise Exception('Cannot find address of the local machine.')
    return addr


def tcp_url(address: str, port: Union[str, int, None] = None) -> str:
    """Construct a tcp url safe for IPv4 and IPv6"""
    if address == "*":
        return "tcp://*"

    ip_addr = ipaddress.ip_address(address)

    port_suffix = f":{port}" if port else ""

    if ip_addr.version == 6 and port_suffix:
        url = f"tcp://[{address}]{port_suffix}"
    else:
        url = f"tcp://{address}{port_suffix}"

    return url
