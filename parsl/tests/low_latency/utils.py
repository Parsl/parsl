import socket
import fcntl
import struct
import subprocess


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


def ping_time(ip, n=5):
    """
    Returns the average ping time in microseconds.

    Note: This function is inherently platform specific. 
    It currently works on Midway.
    """
    cmd = "ping {} -c {}".format(ip, n)
    p = subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE)
    output = str(p.communicate()[0])
    stats = output.split("\n")[-1].split(" = ")[-1].split("/")
    avg_ping_time = float(stats[1]) # In ms
    return avg_ping_time * 1000
    