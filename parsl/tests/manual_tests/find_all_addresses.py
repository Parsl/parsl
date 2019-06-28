from parsl.addresses import *
import psutil
import zmq
import time
from zmq.utils.monitor import recv_monitor_message


def get_all_addresses():
    """ Uses a combination of methods to determine possible addresses.

    Returns:
         list of addresses as strings
    """
    net_interfaces = psutil.net_if_addrs()

    s_addresses = []
    for interface in net_interfaces:
        try:
            s_addresses.append(address_by_interface(interface))
        except Exception as e:
            pass

    s_addresses = set(s_addresses)
    print(s_addresses)

    try:
        s_addresses.add(address_by_route())
        s_addresses.add(address_by_query())
        s_addresses.add(address_by_hostname())
    except Exception as e:
        pass

    s_addresses.discard('127.0.0.1')
    return s_addresses


def find_viable_address(addresses):
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    address_map = {}
    for address in addresses:
        socket = context.socket(zmq.DEALER)
        socket.connect(f"tcp://{address}:50433")
        address_map[address] = {'sock':socket,
                                'mon_sock': socket.get_monitor_socket(events=zmq.EVENT_CONNECTED),
                               }
    first_connected = None
    for i in range(50):
        for address in address_map:
            try:
                recv_monitor_message(address_map[address]['mon_sock'], zmq.NOBLOCK)
                first_connected = address
                break
            except zmq.Again:
                print('Got nothing')
                pass
        if first_connected:
            break
        time.sleep(0.01)

    # Cleanup unused ports
    if first_connected:
        first_socks = address_map.pop(first_connected)
        first_socks['mon_sock'].close()
        for address in address_map:
            address_map[address]['mon_sock'].close()
            address_map[address]['sock'].close()
    return first_connected, first_socks['sock']


def start_test_port(port):
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    port = socket.bind(f"tcp://0.0.0.0:{port}")
    time.sleep(1000)
    print(port)

if __name__ == "__main__":


    addresses = get_all_addresses()
    print(addresses)
    start_test_port(50009)
    #address, socket = get_viable_address()
    #print(f"Address : {address} with socket: {socket}")
