from datetime import datetime, timedelta

from include.configuration import SENSOR_LOGS_TIMESTAMP_COLUMN, CACHE_LOGS_CLEANING_TIME_DELTA_MINUTES


class AutoRemoveOldLogsCache(object):
    
    def __init__(self, storage_lock, cache_storage, logs_ttl_hours=1):
        """
        *DECORATOR*
        The wrapper function will execute a cleaning of the storage after the execution of the decorated function,
        this will happen only if a specified time delta has passed since the last operation (frequency).

        :param storage_lock: Lock necessary to avoid read inconsistencies upon read-write concurrent access
        :param logs_ttl_hours: Amount of days before the elimination of a log entry
        """

        self.__storage_lock = storage_lock
        self.__cache_storage = cache_storage
        self.__logs_ttl = logs_ttl_hours
        self.__cleaning_timedelta = timedelta(minutes=CACHE_LOGS_CLEANING_TIME_DELTA_MINUTES)
        self.__last_cleaning_timestamp = datetime.now() - timedelta(days=999)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)  # Execute the decorated function first (and save possible return values)

            if self.__last_cleaning_timestamp <= (datetime.now() - self.__cleaning_timedelta):
                self.__storage_lock.acquire()  # Storage lock acquisition

                # Define a date threshold according to the logs ttl
                cleaning_threshold = (datetime.now() - timedelta(hours=self.__logs_ttl))

                for log_key in self.__cache_storage.keys():
                    if self.__cache_storage[log_key]["values"].shape[0] > 0:
                        mask = self.__cache_storage[log_key]["values"][SENSOR_LOGS_TIMESTAMP_COLUMN] > cleaning_threshold
                        self.__cache_storage[log_key]["values"] = self.__cache_storage[log_key]["values"][mask].copy()
                        self.__cache_storage[log_key]["last_query_cache_threshold"] = cleaning_threshold
                        print(f"Cleaning cache {log_key} - oldest timestamp: {self.__cache_storage[log_key]['values'][SENSOR_LOGS_TIMESTAMP_COLUMN].min()}")

                self.__storage_lock.release()  # Storage lock release

                self.__last_cleaning_timestamp = datetime.now()
            return ret
        return wrapper
