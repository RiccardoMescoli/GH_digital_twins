import Pyro5.api as pyro
import os
import threading
import paho.mqtt.client as mqtt
import pandas as pd
from datetime import datetime, timedelta
import time
import socket

from include.configuration import *
from include.decorators import AutoRemoveOldLogsHDF5, SetLogHDF5Storage
from include.callbacks import on_sensor_message_hdf5


class DataLogger(object):

    def __init__(self, log_dir, storage_filename, mqtt_broker_ip):

        # == STORAGE SETUP ==
        self.__cache = (dict() if SENSOR_CACHE_SIZE > 1 else None)  # Cache to keep messages before batch-storing
        self.__lock = threading.Lock()                              # Lock to synchronize read and write operations

        self.__log_dir = os.getcwd() + str(log_dir)   # The /log directory will be positioned in the working directory
        if not os.path.isdir(self.__log_dir):         # If the directory doesn't exist, create it
            os.mkdir(self.__log_dir)

        self.__storage = pd.HDFStore(self.__log_dir + "/" + str(storage_filename))   # Creating a HDFS5 storage

        # == MQTT SETUP ==
        # MQTT client setup
        self.__MQTTclient = mqtt.Client()          # MQTT client creation
        self.__MQTTclient.connect(mqtt_broker_ip)  # Connecting the client to the MQTT broker
        self.__MQTTclient.loop_start()             # Starting the thread that processes incoming and outgoing messages
        # MQTT subscriptions setup
        self.__MQTTclient.subscribe(SENSOR_FEED_TOPIC_PATTERN)   # Subscription to the topics regarding sensor feeds
        # MQTT callbacks setup
        # Those are two decorators which also take configuration parameters
        on_sensor_message_callback = AutoRemoveOldLogsHDF5(hdf5_storage=self.__storage,
                                                           storage_lock=self.__lock,
                                                           logs_ttl=STORAGE_TIME_PERIOD_DAYS
                                                           )(SetLogHDF5Storage(storage_lock=self.__lock,
                                                                               log_storage=self.__storage,
                                                                               cache=self.__cache,
                                                                               sensor_cache_size=SENSOR_CACHE_SIZE
                                                                               )(on_sensor_message_hdf5)
                                                             )
        self.__MQTTclient.message_callback_add(SENSOR_FEED_TOPIC_PATTERN,  # Adding a callback for sensor feeds
                                               on_sensor_message_callback
                                               )

    def __del__(self):
        try:
            self.__MQTTclient.loop_stop()  # Clean termination of MQTT message handling
            time.sleep(4)  # Waiting for the loop to stop handling the remaining messages
        except AttributeError:
            pass

        try:
            self.__lock.acquire()  # Acquire lock

            for key in self.__cache.keys():
                # Storing all cached values before closing
                if len(self.__cache[key]) > 0:
                    self.__storage.append(key,
                                          pd.DataFrame(self.__cache[key], columns=SENSOR_LOGS_COLUMNS),
                                          format='t',
                                          append=True,
                                          encoding='utf-8',
                                          data_columns=True,
                                          min_itemsize=SENSOR_LOGS_COLUMNS_SIZES
                                          )
            self.__storage.close()  # Storage flushed and closed
        except AttributeError:
            pass
        finally:
            if self.__lock.locked():
                self.__lock.release()  # Lock release

    def __get_sensor_log_threshold(self, storage_key, threshold):
        try:
            self.__lock.acquire()  # Lock acquisition

            cached_vals = self.__cache.get(storage_key)

            # Data retrieval from storage (first check if the key is in the storage, or it is only in cache)
            if (storage_key if storage_key[0] == "/" else "/" + storage_key) in self.__storage.keys():
                ret = self.__storage.select(storage_key,
                                            SENSOR_LOGS_TIMESTAMP_COLUMN + f" >= '{threshold}'"
                                            ).values.tolist()
            else:
                ret = []

            # If the retrieval from storage returned at least a value, all cached values match the query
            if len(ret) > 0:
                ret = ret + (cached_vals if cached_vals is not None else [])
            elif cached_vals is not None:  # If no storage values are returned, some cached values may match the query
                cached_vals_df = pd.DataFrame(cached_vals, columns=SENSOR_LOGS_COLUMNS)
                mask = cached_vals_df[SENSOR_LOGS_TIMESTAMP_COLUMN] >= threshold
                ret = cached_vals_df[mask].values.tolist()

        except KeyError as remote_error:
            error_string = "(client) KeyError: " + str(remote_error)
            print(error_string)
            raise remote_error
        finally:
            if self.__lock.locked():
                self.__lock.release()  # Lock release

        print(f"ANSWERED QUERY - results: {len(ret)}")

        return ret

    @pyro.expose
    def ping(self):
        return True

    @pyro.expose
    def get_sensor_log(self, source_id, feed_id, days=0, hours=0, minutes=0, seconds=0):
        """
        Remote method to retrieve entries matching a specific time slice from a specific log
        :param source_id: Identifier of the sensor feed source
        :param feed_id: Identifier of the kind of sensor feed (kind of measurement)
        :param days: Time slice to retrieve from the log in days
        :param hours: Time slice to retrieve from the log in hours
        :param minutes: Time slice to retrieve from the log in minutes
        :param seconds: Time slice to retrieve from the log in seconds
        :return: List of the log entries from the selected source and kind of feed matching the selected time slice
        """
        threshold = datetime.now() - timedelta(days=float(days),
                                               hours=float(hours),
                                               minutes=float(minutes),
                                               seconds=float(seconds))

        source_id_str = str(source_id)
        feed_id_str = str(feed_id)
        storage_key = source_id_str + "_" + (feed_id_str.upper() if feed_id_str.isalpha() else feed_id_str)

        return self.__get_sensor_log_threshold(storage_key, threshold)

    @pyro.expose
    def get_sensor_log_till_timestamp(self, source_id, feed_id, timestamp):
        source_id_str = str(source_id)
        feed_id_str = str(feed_id)
        storage_key = source_id_str + "_" + (feed_id_str.upper() if feed_id_str.isalpha() else feed_id_str)

        return self.__get_sensor_log_threshold(storage_key, timestamp)

    @pyro.expose
    def get_sensor_source_logs(self, source_id, days=0, hours=0, minutes=0, seconds=0):
        storage_keys = [key.strip("/") for key in self.__storage.keys()]
        cached_keys = list(self.__cache.keys())
        logs = dict()

        for key in set(storage_keys + cached_keys):
            key_components = key.split("_")
            if key_components[0] == source_id:
                logs[key] = self.get_sensor_log(source_id=key_components[0],
                                                feed_id=key_components[1],
                                                days=days,
                                                hours=hours,
                                                minutes=minutes,
                                                seconds=seconds
                                                )

        return logs

    @pyro.expose
    def get_sensor_source_logs_till_timestamp(self, source_id, timestamp):
        storage_keys = [key.strip("/") for key in self.__storage.keys()]
        cached_keys = list(self.__cache.keys())
        logs = dict()

        for key in set(storage_keys + cached_keys):
            key_components = key.split("_")
            if key_components[0] == source_id:
                logs[key] = self.get_sensor_log_till_timestamp(source_id=key_components[0],
                                                               feed_id=key_components[1],
                                                               timestamp=timestamp
                                                               )
        return logs

    @pyro.expose
    def get_sensor_source_feed_keys(self, source_id):
        return [key.split("_")[1] for key in self.__storage.keys() if key.startswith("/" + str(source_id))]

    @pyro.expose
    def get_current_time(self):
        return datetime.now()

def get_ip_address():
    # get the hostname
    hostname = socket.gethostname()
    # get the IP address for the hostname
    ip_address = socket.gethostbyname(hostname)
    return ip_address

# if ON_DOCKER_HOST:
#    daemon = pyro.Daemon(host="172.17.0.1")
# else:
#    daemon = pyro.Daemon(host=get_ip_address())

daemon = pyro.Daemon(host=get_ip_address(), port=LOGGER_PORT)
ns = pyro.locate_ns()
MQTT_BROKER_IP = os.getenv(BROKER_IP_ENV_VAR)
print(f"Broker IP: {MQTT_BROKER_IP}")
logger_obj = DataLogger(LOG_DIR, LOG_STORAGE, MQTT_BROKER_IP)
print("LOGGER OBJ CREATED")
uri = daemon.register(logger_obj)
ns.register(str(uri), uri, metadata=DATA_LOGGER_METADATA)
print("OBJ REGISTERED ON NS")
print(f"Data logger {uri}: READY")
daemon.requestLoop()
