import Pyro5.api as pyro
from Pyro5.errors import CommunicationError, ProtocolError
import threading
from random import randint
from datetime import datetime, timedelta
import pandas as pd
import time

from include.configuration import *


class GHBlockDT(object):

    def __init__(self, greenhouse_id, block_id):
        self.__greenhouse_id = str(greenhouse_id)
        self.__block_id = str(block_id)
        self.__id = self.__greenhouse_id + self.__block_id
        self.__network_id = None  # To be initialized with an appropriate call

        self.__lock = threading.Lock()

        self.__is_master = False
        self.__master_id = None
        # TODO: MAKE A DECORATOR THAT CLEANS THE CACHE SIMILARLY TO THE ONE ON THE DATA LOGGER (is this necessary??)
        self.__cache = dict()

    @staticmethod
    def __iso_format_col_to_datetime(df, column):
        new_column = df[column].apply(lambda x: datetime.fromisoformat(x))
        df[column] = new_column
        return df

    def set_network_id(self, network_id):
        self.__network_id = network_id

    def lookup_master(self, startup=False):
        master_id = None
        no_peers = True

        # Get a list of all analogous digital twins
        peer_list = list(pyro.locate_ns().yplookup(meta_all=[
            "DT:GH_block",
            "greenhouse:" + self.__greenhouse_id,
            "block:" + self.__block_id
        ]
        ).keys()
                         )

        # If there are other digital twins of the same kind
        if len(peer_list) > 0:
            # Try to get the master ID from the peers
            while master_id is None and len(peer_list) > 0:
                proxy_name = peer_list.pop(randint(0, len(peer_list) - 1))

                try:
                    with pyro.Proxy(proxy_name) as proxy:
                        master_id = proxy.get_master_id()
                        no_peers = False
                except CommunicationError:
                    print("ERROR: tried master lookup on unavailable peer")

        if no_peers:  # If this is the only digital twin of its kind it elects itself master
            self.__is_master = True
            self.__master_id = self.__network_id
        elif master_id is None:  # If the digital twin had peers but none of them knew the master start the election
            self.initiate_leader_election(initiator=True, startup=startup)
        else:  # Otherwise, just set the new master ID and check if it is still available
            self.__master_id = str(master_id)
            try:
                with pyro.Proxy(f"PYROMETA:{NETWORK_ID_METADATA_KEY + master_id}") as master_proxy:
                    master_proxy.ping()
            except CommunicationError:
                print("ERROR: Master node unavailable")
                self.initiate_leader_election(initiator=True, startup=startup)
        print(f" ================ MASTER LOOKUP ENDED -- MASTER: {self.__master_id}\n")

    @pyro.oneway
    @pyro.expose
    def initiate_leader_election(self, initiator=False, startup=False):
        if not initiator and startup:  # Ensures that every node currently online is visible on the ns
            time.sleep(ELECTION_INITIATION_DELAY_SECONDS)

        print(f" ================ LEADER ELECTION INITIATED -- INITIATOR: {initiator}")
        self.__master_id = None
        self.__is_master = False
        contenders_id_list = list([self.__network_id])

        # Get a list of all analogous digital twins
        peer_meta = pyro.locate_ns().yplookup(meta_all=[
            "DT:GH_block",
            "greenhouse:" + self.__greenhouse_id,
            "block:" + self.__block_id
        ]
        )
        peer_list = list(peer_meta.keys())
        print(peer_list)
        # For each peer, try to contact it to get its network ID
        for proxy_name in peer_list:
            proxy_is_self = False
            # Check you are not trying to contact yourself
            for meta in peer_meta[proxy_name][1]:
                if meta == NETWORK_ID_METADATA_KEY + self.__network_id:
                    proxy_is_self = True

            if not proxy_is_self:
                with pyro.Proxy(proxy_name) as proxy:
                    proxy._pyroMaxRetries = ELECTION_CONTACT_RETRY_ATTEMPTS  # Set the number of retries in case of
                    peer_id = None                                           # communication failure

                    try:
                        peer_id = proxy.get_network_id()
                        if initiator:  # If the initiator is executing it will also activate the other nodes
                            proxy.initiate_leader_election(startup=startup)
                    except CommunicationError:
                        print(f"ERROR: contacted unavailable peer during leader election - {proxy_name}")

                    if peer_id is not None:  # If the peer was contacted successfully, add its net-ID to the list
                        contenders_id_list.append(peer_id)

        self.__master_id = min(contenders_id_list)  # Set the master as the contender with the smallest net-ID
        if self.__master_id == self.__network_id:  # If the master net-ID is the same as the node executing, set the
            self.__is_master = True  # is_master flag to True

        print(contenders_id_list)
        print(f" ================ LEADER ELECTION ENDED -- MASTER: {self.__is_master}\n"
              f" ================ MASTER_ID: {self.__master_id}\n")

    # TODO: FIX THE CODE REPETITION ISSUE IN THIS METHOD
    def handle_query_forwarding(self, proxy, feed_id, days=0.0, hours=0.0, minutes=0.0, seconds=0.0):
        error = None
        received_log = None
        for i in range(QUERY_FORWARDING_MAX_ATTEMPTS):
            try:
                if error is not None and self.__is_master:          # In case the node gets elected as Master
                    received_log = proxy.get_sensor_log(self.__id,  # contact a logger directly
                                                        feed_id,
                                                        days=days,
                                                        hours=hours,
                                                        minutes=minutes,
                                                        seconds=seconds
                                                        )
                else:
                    received_log = proxy.forward_query(feed_id, days=days, hours=hours, minutes=minutes, seconds=seconds)
                break
            except ProtocolError as e:
                print(" >> ERROR: Tried forwarding to another SLAVE node")
                self.lookup_master()
                if self.__is_master:  # In case the contacted node was actually a slave, lookup the real master
                    datalogger_names = list(pyro.locate_ns().yplookup(meta_all=["datalogger"]).keys())
                    logger_proxy_name = datalogger_names[randint(0, len(datalogger_names) - 1)]
                    proxy = pyro.Proxy(logger_proxy_name)
                else:
                    time.sleep(QUERY_FORWARDING_REDIRECTION_DELAY)
                    proxy = pyro.Proxy("PYROMETA:" + NETWORK_ID_METADATA_KEY + self.__master_id)
                error = e
            except CommunicationError as e:  # In case the master goes down, initiate a new leader election
                print(" >> ERROR: Tried forwarding to unavailable MASTER node")
                self.initiate_leader_election(initiator=True)
                if self.__is_master:
                    datalogger_names = list(pyro.locate_ns().yplookup(meta_all=["datalogger"]).keys())
                    logger_proxy_name = datalogger_names[randint(0, len(datalogger_names) - 1)]
                    proxy = pyro.Proxy(logger_proxy_name)
                else:
                    time.sleep(QUERY_FORWARDING_REDIRECTION_DELAY)
                    proxy = pyro.Proxy("PYROMETA:" + NETWORK_ID_METADATA_KEY + self.__master_id)
                error = e

        if error is not None and received_log is None:
            raise error

        return received_log

    @pyro.expose
    def ping(self):
        return True

    @pyro.expose
    def get_network_id(self):
        return self.__network_id

    @pyro.expose
    def get_master_id(self):
        return self.__master_id

    @pyro.expose
    def forward_query(self, feed_id, days=0.0, hours=0.0, minutes=0.0, seconds=0.0):
        """
        Slave nodes can call this method on the master to forward a query to it, taking advantage of the master
        cache
        :param feed_id:
        :param days:
        :param hours:
        :param minutes:
        :param seconds:
        :return:
        """
        if not self.__is_master:
            raise ProtocolError

        print(" -- EXECUTING QUERY FOR A SLAVE NODE")
        return self.get_sensor_log(feed_id, days=days, hours=hours, minutes=minutes, seconds=seconds)

    def query_discard_cache(self, proxy, feed_id, days, hours, minutes, seconds):
        print("START QUERY - DISCARD")
        if self.__is_master:
            print(" >> DIRECT QUERY")
            # TODO: handle the case with unavailable logger
            received_log = proxy.get_sensor_log(self.__id,
                                                feed_id,
                                                days=days,
                                                hours=hours,
                                                minutes=minutes,
                                                seconds=seconds
                                                )
        else:
            print(" >> FORWARDING QUERY TO MASTER")
            received_log = self.handle_query_forwarding(proxy,
                                                        feed_id,
                                                        days=days,
                                                        hours=hours,
                                                        minutes=minutes,
                                                        seconds=seconds
                                                        )

        self.__cache[feed_id] = dict()
        received_dataframe = pd.DataFrame(received_log, columns=SENSOR_LOGS_COLUMNS)
        # Convert the timestamp column from string (iso format) to datetime
        received_dataframe = self.__iso_format_col_to_datetime(received_dataframe,
                                                               SENSOR_LOGS_TIMESTAMP_COLUMN
                                                               )
        self.__lock.acquire()
        self.__cache[feed_id]["values"] = received_dataframe
        self.__lock.release()

        print(" >> DISCARDED CACHED DATA!")
        return received_dataframe

    def query_keep_cache(self, cached_log, proxy, feed_id, current_timestamp, remote_current_timestamp, delta):
        print("START QUERY - REUSE")

        # Tries to get the data logs successive to last query (minus a second for possible discrepancies)
        query_timestamp = cached_log["update_timestamp"] - cached_log["relative_delta"] - timedelta(seconds=1)
        if self.__is_master:
            print(" >> DIRECT QUERY")
            received_log = proxy.get_sensor_log_till_timestamp(self.__id,
                                                               feed_id,
                                                               query_timestamp
                                                               )
        else:
            print(" >> FORWARDING QUERY TO MASTER")
            query_delta = current_timestamp - query_timestamp
            received_log = self.handle_query_forwarding(proxy,
                                                        feed_id,
                                                        days=query_delta.days,
                                                        seconds=query_delta.seconds + (query_delta.microseconds
                                                                                       / int(1e+6))
                                                        )

        # Turn the data into a dataframe and convert the timestamp column from string (iso format) to datetime
        received_dataframe = self.__iso_format_col_to_datetime(pd.DataFrame(received_log,
                                                                            columns=SENSOR_LOGS_COLUMNS
                                                                            ),
                                                               SENSOR_LOGS_TIMESTAMP_COLUMN
                                                               )

        self.__lock.acquire()
        # Concatenate the newly obtained data with the previously existing cached data
        self.__cache[feed_id]["values"] = pd.concat([cached_log["values"],
                                                     received_dataframe
                                                     ], ignore_index=True).drop_duplicates(ignore_index=True)

        # Get the current time from the logger in order to give back an accurate timeslice
        mask = self.__cache[feed_id]["values"][SENSOR_LOGS_TIMESTAMP_COLUMN] >= remote_current_timestamp - delta
        ret = self.__cache[feed_id]["values"][mask]
        self.__lock.release()

        print(" >> REUSED CACHED DATA!")
        return ret

    @pyro.expose
    def get_sensor_log(self, feed_id, days=0.0, hours=0.0, minutes=0.0, seconds=0.0):
        datalogger_names = list(pyro.locate_ns().yplookup(meta_all=["datalogger"]).keys())

        logger_proxy_name = datalogger_names[randint(0, len(datalogger_names) - 1)]

        current_timestamp = datetime.now()
        delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        cache_threshold = current_timestamp - delta
        cached_log = self.__cache.get(feed_id)
        log_timestamps = cached_log["values"][SENSOR_LOGS_TIMESTAMP_COLUMN] if cached_log is not None else None

        logger_proxy = pyro.Proxy(logger_proxy_name)

        # Getting the current time on the server
        # TODO: handle the case with unavailable logger
        remote_current_timestamp = datetime.fromisoformat(logger_proxy.get_current_time())

        if cached_log is not None and type(log_timestamps.min()) != float:
            remote_update_timestamp = (cached_log["update_timestamp"] - cached_log["relative_delta"])

            # THESE CONDITIONS WILL BE USED IN THE FOLLOWING IF STATEMENT

            # If the cache threshold is older than the oldest cached data I discard the cached data and query
            # directly (exception to this rule below)
            threshold_older_than_all_cache = (remote_update_timestamp - log_timestamps.min() <=
                                              cached_log["update_timestamp"] - cache_threshold)
            # UNLESS the last query requested data older or as old as the current request (the threshold is older
            # than the oldest cached data because there is no data in that time period)
            last_cache_threshold_older_than_current = (cached_log["last_query_cache_threshold"] <= cache_threshold)
        else:  # The following values are just standard assignments that will never be truly used
            threshold_older_than_all_cache = True
            last_cache_threshold_older_than_current = False

        # If either there are no cached logs, the cached logs are too old, or the oldest cached log
        # is more recent than the required threshold, just ask for the latest values in the requested time-window
        if self.__is_master:
            proxy = logger_proxy
        else:
            proxy = pyro.Proxy("PYROMETA:" + NETWORK_ID_METADATA_KEY + self.__master_id)
            del logger_proxy

        if (cached_log is None
                or cached_log["update_timestamp"] <= cache_threshold
                or type(log_timestamps.min()) == float  # The function may return float NaN if the cache is empty
                or (threshold_older_than_all_cache and not last_cache_threshold_older_than_current)):

            ret = self.query_discard_cache(proxy, feed_id, days, hours, minutes, seconds)

        else:  # If the cached values cover the older end of the time-window just ask for the remaining values
            ret = self.query_keep_cache(cached_log, proxy, feed_id, current_timestamp, remote_current_timestamp, delta)

        # Update the "last update" timestamp
        self.__cache[feed_id]["update_timestamp"] = current_timestamp
        # Update the time delta between the local time and server time
        self.__cache[feed_id]["relative_delta"] = current_timestamp - remote_current_timestamp
        # Update the requested time delta of the last executed query
        self.__cache[feed_id]["last_query_cache_threshold"] = cache_threshold

        return ret.values.tolist()


daemon = pyro.Daemon()
ns = pyro.locate_ns()
gh_block_obj = GHBlockDT(GREENHOUSE_ID, BLOCK_ID)
uri = daemon.register(gh_block_obj)

# The network ID of the digital twin is extracted directly from its URI
net_id = str(uri).split('_')[1].split('@')[0]
gh_block_obj.set_network_id(network_id=net_id)
GH_BLOCK_METADATA.add(NETWORK_ID_METADATA_KEY + net_id)

gh_block_obj.lookup_master(startup=True)
ns.register(str(uri), uri, metadata=GH_BLOCK_METADATA)
print(f"Greenhouse block {uri}: READY")
print(f"Network ID: {net_id}\n")
daemon.requestLoop()
