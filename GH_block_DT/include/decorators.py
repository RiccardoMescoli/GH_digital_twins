from datetime import datetime, timedelta

from include.configuration import SENSOR_LOGS_TIMESTAMP_COLUMN


class AutoRemoveOldLogsHDF5(object):
    
    def __init__(self, storage_lock, logs_ttl_hours=1):
        """
        *DECORATOR*
        The wrapper function will execute a cleaning of the storage after the execution of the decorated function,
        this will happen only if a specified time delta has passed since the last operation (frequency).

        :param storage_lock: Lock necessary to avoid read inconsistencies upon read-write concurrent access
        :param logs_ttl_hours: Amount of days before the elimination of a log entry
        """

        self.__storage_lock = storage_lock
        self.__logs_ttl = logs_ttl_hours
        self.__cleaning_timedelta = timedelta(hours=1)
        self.__last_cleaning_timestamp = datetime.now() - timedelta(days=999)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)  # Execute the decorated function first (and save possible return values)

            # BODY #

            return ret
        return wrapper
