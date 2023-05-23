import os
from datetime import datetime
import pandas as pd

from include.configuration import SENSOR_LOGS_COLUMNS, SENSOR_LOGS_COLUMNS_SIZES


def on_sensor_message(client, userdata, message, log_dir):

    topic = message.topic.split("/")
    try:
        sensor_type = topic[topic.index("f") + 1].upper()
        log_file = log_dir + "/" + str(datetime.now().date()) + "_" + sensor_type + ".csv"

        try:
            # Log file creation
            if not os.path.isfile(log_file):  # If the log file doesn't exist create it from scratch
                log = open(log_file, 'w')
                log.write("value, datetime\n")
            else:
                log = open(log_file, 'a')     # If the log file already exists append the new values

            log.write(f"{message.payload.decode('utf-8')}, {datetime.now()}\n")
            log.close()
        except OSError:
            print(f'ERROR: failed to open {log_file}')

    except (ValueError, IndexError):
        print('ERROR: "on_sensor_message" callback received a message from a topic with incompatible structure')


def on_sensor_message_hdf5(client, userdata, message, log_storage, cache, sensor_cache_size, storage_lock):

    topic = message.topic.split("/")  # Get the topic as a list of strings representing each topic level
    try:
        # The last level is assumed to represent the type of sensor value
        _id_begin = topic.index("greenhouses")

        # Log key definition
        greenhouse_id = topic[_id_begin + 1] + topic[_id_begin + 2]
        sensor_type = topic[topic.index("f") + 1].upper()
        log_key = greenhouse_id + "_" + sensor_type

        # Creation of the new tuple
        new_entry = (message.payload.decode('utf-8'), datetime.now())

        if cache is not None:  # If a cache has been defined fill it before spilling to the actual storage
            sensor_cache = cache.get(log_key)

            if sensor_cache is not None:
                sensor_cache.append(new_entry)

                if len(sensor_cache) >= sensor_cache_size:
                    storage_lock.acquire()  # Acquire the lock before writing (this way there won't be any faulty reads)

                    # Put the cached entries in the storage then flush the modifications to disk
                    log_storage.append(log_key,
                                       pd.DataFrame(sensor_cache, columns=SENSOR_LOGS_COLUMNS),
                                       format='t',
                                       append=True,
                                       encoding='utf-8',
                                       data_columns=True,
                                       min_itemsize=SENSOR_LOGS_COLUMNS_SIZES
                                       )
                    log_storage.flush(fsync=True)

                    storage_lock.release()  # Release the lock after the writing operation ended

                    cache[log_key] = list()  # Clean the cache
            else:  # The cache size is greater than one, so a new cache can't be spilled upon creation
                cache[log_key] = list((new_entry,))

        else:  # If no cache has been defined just put the value in the memory
            storage_lock.acquire()
            log_storage.append(log_key,
                               pd.DataFrame(new_entry, columns=SENSOR_LOGS_COLUMNS),
                               format='t',
                               append=True,
                               encoding='utf-8',
                               data_columns=True
                               )
            log_storage.flush(fsync=True)
            storage_lock.release()

    except (ValueError, IndexError) as error:
        print('ERROR: "on_sensor_message" callback received a message from a topic with incompatible structure\n',
              error)
    finally:
        if storage_lock.locked():
            storage_lock.release()