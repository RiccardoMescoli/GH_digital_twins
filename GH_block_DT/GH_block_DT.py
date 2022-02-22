import Pyro5.api as pyro
from random import randint

import pandas as pd

from include.configuration import *


class GHBlockDT(object):

    def __init__(self, greenhouse_id, block_id):
        self.__greenhouse_id = str(greenhouse_id)
        self.__block_id = str(block_id)
        self.__id = self.__greenhouse_id + self.__block_id

        self.__is_master = False

        self.__cache = dict()

    # TODO: define a way to lookup the master
    def __lookup_master(self):
        pass

    # TODO: turn the received datastructure into a pandas dataframe or a set of pandas dataframes
    # TODO: define a way for the DT to retrieve only the values missing from the cache (the cache may have a subset)
    @pyro.expose
    def get_sensor_values(self, feed_id, days=0, hours=0, minutes=0, seconds=0):
        datalogger_names = list(pyro.locate_ns().yplookup(meta_all=["datalogger"]).keys())

        proxy_name = datalogger_names[randint(0, len(datalogger_names) - 1)]
        with pyro.Proxy(proxy_name) as proxy:
            received_log = proxy.get_sensor_log(self.__id,
                                                feed_id,
                                                days=days,
                                                hours=hours,
                                                minutes=minutes,
                                                seconds=seconds
                                                )
            self.__cache[feed_id] = pd.DataFrame(received_log, columns=SENSOR_LOGS_COLUMNS)
        return received_log


daemon = pyro.Daemon()
ns = pyro.locate_ns()
gh_block_obj = GHBlockDT(GREENHOUSE_ID, BLOCK_ID)
uri = daemon.register(gh_block_obj)

ns.register(str(uri), uri, metadata=GH_BLOCK_METADATA)
print(f"Greenhouse block {uri}: READY")
daemon.requestLoop()
