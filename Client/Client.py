import Pyro5.api as pyro
from Pyro5.errors import PyroError, NamingError, CommunicationError

from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, Select
from bokeh.layouts import layout, column
from bokeh.plotting import figure

import pandas as pd

from datetime import datetime, timedelta
from math import radians
from random import randint
import time

from include.configuration import *


def _gh_block_dt_proxy_generator(block_id):
    ns = pyro.locate_ns()

    dt_list = list(ns.yplookup(meta_all=["DT:GH_block",
                                         "greenhouse:" + block_id.value[0],
                                         "block:" + block_id.value[1]
                                         ]
                               ).keys())

    while len(dt_list) > 0:
        dt_name = dt_list.pop(randint(0, len(dt_list) - 1))
        dt_proxy = pyro.Proxy(dt_name)
        yield dt_proxy


# Creating a preriodic update function
def update():
    global latest_timestamp
    global select_block_id
    global select_sensor
    global source

    proxy_gen = _gh_block_dt_proxy_generator(select_block_id)

    current_try = 0

    for rem_source in proxy_gen:
        try:
            new_data_df = pd.DataFrame(rem_source.get_sensor_log(select_sensor.value,
                                                                 seconds=30
                                                                 ),
                                       columns=STREAM_DATA_COLUMNS
                                       )

            new_data_df["values"] = new_data_df["values"].astype(float)
            new_data_df["timestamp"] = new_data_df["timestamp"].apply(lambda x: datetime.fromisoformat(x))
            new_data_df = new_data_df[new_data_df["timestamp"] > latest_timestamp]
            latest_timestamp = new_data_df["timestamp"].min() + timedelta(seconds=MIN_DELTA_BETWEEN_SAMPLES_SECS)
            new_data = new_data_df.to_dict(orient="list")

            source.stream(new_data, rollover=GRAPH_ROLLOVER)

            break

        except CommunicationError:
            print("ERROR: Failed communication with the DT")

            current_try += 1
            if current_try >= GET_LOGS_MAX_TRIES:
                print("ERROR: Failed to retrieve new data")
                break


def _initialize_graph_data(rem_source):
    global source
    global plot
    global latest_timestamp

    try:
        new_data = pd.DataFrame(rem_source.get_sensor_log(select_sensor.value, hours=1), columns=["values", "timestamp"])
        new_data["values"] = new_data["values"].astype(float)
        new_data["timestamp"] = new_data["timestamp"].apply(lambda x: datetime.fromisoformat(x))
        source.data = new_data.to_dict(orient="list")
        plot.title.text = f"Block: {select_block_id.value}  Sensor: {select_sensor.value}"
        latest_timestamp = new_data["timestamp"].min()

        return True
    except NamingError:
        print("ERROR: Failed to find the DT")
        return False
    except CommunicationError:
        print("ERROR: Failed communication with the DT")
        return False


def change_source(attrname, old, new):
    proxy_gen = _gh_block_dt_proxy_generator(select_block_id)

    for rem_source in proxy_gen:
        if _initialize_graph_data(rem_source):
            break
        else:
            print("ERROR: Source change failed!")


# Plot creation
plot = figure(x_axis_type="datetime", width=900, height=350)

# Selectors
select_sensor = Select(title="Sensor", value="TEMPERATURE", options=SENSOR_OPTIONS)
select_sensor.on_change("value", change_source)

select_block_id = Select(title="Greenhouse block", value=BLOCK_ID_OPTIONS[0][0], options=BLOCK_ID_OPTIONS)
select_block_id.on_change("value", change_source)

plot.title.text = f"Block: {select_block_id.value}  Sensor: {select_sensor.value}"

# Obtaining the initial data
remote_source = pyro.Proxy("PYROMETA:DT:GH_block,greenhouse:" + BLOCK_ID_OPTIONS[0][0][0] +
                           ",block:" + BLOCK_ID_OPTIONS[0][0][1])
success = True


# Defining the data source object
source = ColumnDataSource()
# Defining the variable keeping track of the latest sensor value available
latest_timestamp = datetime.min

# Initializing the graph for the first time
while not _initialize_graph_data(remote_source):
    print("Failed to get the initial set of data! Retrying . . .")
    time.sleep(3)

# Defining the plots
plot.circle(x="timestamp", y="values", color="firebrick", line_color="firebrick", source=source)
plot.line(x="timestamp", y="values", source=source)

date_pattern = ["%Y-%m-%d\n%H:%M:%S"]

plot.xaxis.formatter = DatetimeTickFormatter(seconds=date_pattern,
                                             minsec=date_pattern,
                                             minutes=date_pattern,
                                             hourmin=date_pattern,
                                             hours=date_pattern,
                                             days=date_pattern,
                                             months=date_pattern,
                                             years=date_pattern
                                             )


plot.xaxis.major_label_orientation = radians(80)
plot.xaxis.axis_label = "Datetime"
plot.yaxis.axis_label = "Values"

# Config layout
lay = layout(column(plot, select_block_id, select_sensor))
curdoc().add_root(lay)
curdoc().add_periodic_callback(update, UPDATE_PERIOD)
