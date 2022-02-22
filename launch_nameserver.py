from Pyro5 import config, nameserver
config.NS_AUTOCLEAN = 30
nameserver.start_ns_loop()
