import os
from datetime import datetime, timedelta

from include.configuration import SENSOR_LOGS_TIMESTAMP_COLUMN


def _process_log_dir_str(log_dir):
    dir_name = str(log_dir)

    if len(dir_name) == 0:
        dir_name = os.getcwd() + "/log"
    if dir_name[0] != "/":
        dir_name = "/" + dir_name

    return dir_name


def _process_log_hdf5_storage_str(log_storage):
    storage_name = str(log_storage)

    if len(storage_name) == 0:
        storage_name = "/logs.h5"
    elif storage_name[0] != "/":
        storage_name = "/" + storage_name

    return storage_name


class SetLogDir(object):

    def __init__(self, log_dir="/log"):
        self.__log_dir = _process_log_dir_str(log_dir)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            return func(log_dir=self.__log_dir, *args, **kwargs)
        return wrapper


class SetLogHDF5Storage(object):

    def __init__(self, storage_lock, log_storage, cache=None, sensor_cache_size=10):
        """
        *DECORATOR*
        Sets a wrapper function around a callback, setting automatically all the configuration variables and objects
        necessary to access a HDFS5 storage through pandas and pytables
        :param storage_lock: Lock necessary to avoid read inconsistencies upon read-write concurrent access
        :param log_storage: Storage object which will keep the logs
        :param cache: Cache used to store new log entries and store them in batch when filled
        :param sensor_cache_size: Maximum cache size, once reached the cache is spilled to the storage
        """
        self.__storage_lock = storage_lock
        self.__hdf5_storage = log_storage
        self.__cache = cache
        self.__sensor_cache_size = sensor_cache_size

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            return func(log_storage=self.__hdf5_storage,
                        cache=self.__cache,
                        sensor_cache_size=self.__sensor_cache_size,
                        storage_lock=self.__storage_lock,
                        *args, **kwargs)
        return wrapper


class SetLogSensorType(object):

    def __init__(self, sensor_type=""):
        self.__sensor_type = str(sensor_type).upper()

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            return func(sensor_type=self.__sensor_type, *args, **kwargs)
        return wrapper


class AutoRemoveOldLogsHDF5(object):
    
    def __init__(self, hdf5_storage, storage_lock, logs_ttl_days=7):
        """
        *DECORATOR*
        The wrapper function will execute a cleaning of the storage after the execution of the decorated function,
        this will happen only if a specified time delta has passed since the last operation (frequency).

        :param hdf5_storage: Storage object to access the logs
        :param storage_lock: Lock to synchronize accesses to the storage (in order to avoid faulty reads)
        :param logs_ttl_days: Amount of days before the elimination of a log entry
        """

        self.__hdf5_storage = hdf5_storage
        self.__storage_lock = storage_lock
        self.__logs_ttl = logs_ttl_days
        self.__cleaning_timedelta = timedelta(days=1)
        self.__last_cleaning_timestamp = datetime.now() - timedelta(days=999)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)  # Execute the decorated function first (and save possible return values)

            if self.__last_cleaning_timestamp <= (datetime.now() - self.__cleaning_timedelta):
                self.__storage_lock.acquire()  # Storage lock acquisition

                # Define a date threshold according to the logs ttl
                threshold = (datetime.now().date() - timedelta(days=self.__logs_ttl))

                for log_key in self.__hdf5_storage.keys():  # For each sensor log
                    # Remove all records older than the specified threshold
                    self.__hdf5_storage.remove(log_key, where=SENSOR_LOGS_TIMESTAMP_COLUMN + f" <= '{threshold}'")
                    self.__hdf5_storage.flush(fsync=True)  # Flush the changes to disk

                self.__storage_lock.release()  # Storage lock release

                self.__last_cleaning_timestamp = datetime.now()

            return ret
        return wrapper
