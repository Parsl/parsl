import socket
import fcntl
import struct


def get_ip_address(ifname):
    """
    Returns the IP address of the given ifname, e.g. 'eth0'
    
    Taken from this Stack Overflow answer: https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-of-eth0-in-python#24196955
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
    )[20:24])
