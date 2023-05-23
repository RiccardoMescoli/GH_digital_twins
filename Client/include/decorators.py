import os
from datetime import datetime, timedelta

from include.configuration import SENSOR_LOGS_TIMESTAMP_COLUMN


# TODO: Check if the decorator works and integrate it in the DT class
class AutoRemoveOldLogsHDF5(object):
    
    def __init__(self, hdf5_storage, storage_lock, logs_ttl=7):
        """
        *DECORATOR*
        The wrapper function will execute a cleaning of the storage after the execution of the decorated function,
        this will happen only if a specified time delta has passed since the last operation (frequency).

        :param hdf5_storage: Storage object to access the logs
        :param storage_lock: Lock to synchronize accesses to the storage (in order to avoid faulty reads)
        :param logs_ttl: Amount of days before the elimination of a log entry
        """

        self.__hdf5_storage = hdf5_storage
        self.__storage_lock = storage_lock
        self.__logs_ttl = logs_ttl
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
