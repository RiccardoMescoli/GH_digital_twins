from Pyro5 import config, nameserver
import socket
import os


def get_ip_address():
    # get the hostname
    hostname = socket.gethostname()
    # get the IP address for the hostname
    ip_address = socket.gethostbyname(hostname)
    return ip_address


config.NS_AUTOCLEAN = 5
nameserver.start_ns_loop(host="0.0.0.0")
