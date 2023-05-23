import Pyro5.api as pyro
from time import time
from random import randint

sensors = ["LUX", "TEMPERATURE", "AIRHUMIDITY"]
dt_proxies = [pyro.Proxy(f"PYROMETA:DT:GH_block,greenhouse:A,block:1"),
              pyro.Proxy(f"PYROMETA:DT:GH_block,greenhouse:A,block:2"),
              pyro.Proxy(f"PYROMETA:DT:GH_block,greenhouse:B,block:1"),
              pyro.Proxy(f"PYROMETA:DT:GH_block,greenhouse:B,block:2"),
              ]
while True:
    try:
        dt_proxies[randint(0, len(dt_proxies)-1)].get_sensor_log(sensors[randint(0, len(sensors)-1)], hours=24)
        time.sleep(0.01)
    except Exception as e:
        pass