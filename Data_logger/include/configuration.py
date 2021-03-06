LOG_DIR = "/log"
LOG_STORAGE = "sensor_logs.h5"
SENSOR_CACHE_SIZE = 10
SENSOR_LOGS_TIMESTAMP_COLUMN = "timestamp"
SENSOR_LOGS_COLUMNS = list(["values", SENSOR_LOGS_TIMESTAMP_COLUMN])
SENSOR_LOGS_COLUMNS_SIZES = {"values": 15}
STORAGE_TIME_PERIOD_DAYS = 7
MQTT_BROKER_IP = "localhost"
SENSOR_FEED_TOPIC_PATTERN = "greenhouses/+/+/sensors/f/+"
DATA_LOGGER_METADATA = {"datalogger"}
