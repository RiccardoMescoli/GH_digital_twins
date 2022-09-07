import paho.mqtt.client as mqtt
import time
from numpy.random import normal

client = mqtt.Client("sensor_data_sim")
client.connect("localhost")
sensor_variables = {"temperature": (30, 0.8), "lux": (10000, 100), "airhumidity": (80, 1)}
greenhouses = ["A", "B"]
blocks_for_each_GH = ["1", "2"]

while True:
    for gh in greenhouses:
        for block in blocks_for_each_GH:
            for sensor in sensor_variables.keys():
                topic = "greenhouses/" + gh + "/" + block + "/sensors/f/" + sensor
                value = normal(sensor_variables[sensor][0], sensor_variables[sensor][1])
                client.publish(topic,
                               value
                               )
                print(topic + " --- " + str(value))
    print("\n================================================================================================\n")
    time.sleep(10)
