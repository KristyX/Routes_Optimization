import pandas as pd
import numpy as np
import locator_helper
import datetime
import os
import json
import get_global_id
import copy

def drop_by_time_recalculator(route, orders):
    """Recalculate drop by time at each location, this function will solve non-optimal solution"""
    orders_table = pd.DataFrame(orders)
    source = route["driver_location"]
    route_plan = route["route_plan"]
    current_time = route["driver_start_time"]
    for drop in route_plan:
        order = orders_table[drop["order_id"] == orders_table["id"]]
        destination = drop["drop_by"]
        travel_time, distance = locator_helper.get_trip_time_distance(source, destination)
        current_time += travel_time
        drop["drop_by_start_time"] = current_time
        drop["distance"] = distance
        if drop["drop_by_type"] == "NS" or drop["drop_by_type"] == "S":
            drop["drop_by_end_time"] = order["cookingtime_result"].values.tolist()[0]
            if current_time > drop["drop_by_end_time"]:
                drop["drop_by_end_time"] = current_time
        else:
            drop["drop_by_end_time"] = drop["drop_by_start_time"] + 7*60
        current_time = drop["drop_by_end_time"]
    return route

def _drop_by_time_recalculator(route, orders_table):
    """Recalculate drop by time at each location, this function will solve non-optimal solution"""
    source = route["driver_location"]
    route_plan = route["route_plan"]
    current_time = route["driver_start_time"]
    for drop in route_plan:
        order = orders_table[drop["order_id"] == orders_table["id"]]
        destination = drop["drop_by"]
        travel_time, distance = locator_helper.get_trip_time_distance(source, destination)
        current_time += travel_time
        drop["drop_by_start_time"] = current_time
        drop["distance"] = distance
        if drop["drop_by_type"] == "NS" or drop["drop_by_type"] == "S":
            drop["drop_by_end_time"] = order["cookingtime_result"].values.tolist()[0]
            if current_time > drop["drop_by_end_time"]:
                drop["drop_by_end_time"] = current_time
        else:
            drop["drop_by_end_time"] = drop["drop_by_start_time"] + 7*60
        current_time = drop["drop_by_end_time"]
    return route

def calculate_new_location(driver, B, t):
    """Determine the exact location of the driver after time t"""
    # Case1: driver is waiting at B
    loc = B["drop_by"]
    # Case2: driver is driving to B
    if driver["current_time"] < B["drop_by_start_time"]:
        A = [driver["lat"],driver["lng"]]
        loc = locator_helper.calculate_new_location(A, loc, t)
    return loc

def c(driver, route, current_time):
    # find next point B
    t = current_time - driver["current_time"]
    for cur in route["route_plan"]:
        # check if driver already passed cur location
        if cur["drop_by_end_time"] < current_time:
            driver["lat"], driver["lng"] = cur["drop_by"]
        else:
            # time passed
            return calculate_new_location(driver, cur, t)

def random_walk(driver,plaza, t):
    """Return the estimate location between driver to the closest plaza center"""
    driver_location  = [driver["lat"], driver["lng"]]
    A = np.array(driver_location).astype(float).reshape((-1,2))
    travel_time = locator_helper.calculate_driving_time(A, plaza)
    B = plaza[np.argmin(travel_time)].astype(str).tolist()
    new_location = locator_helper.calculate_new_location(driver_location, B, t)
    return new_location

def update_driver_order(driver, drop_by):
    """Update driver's order info if driver passes drop_by"""
    order_id = drop_by["order_id"]
    order_type = drop_by["drop_by_type"]
    j = 0
    while j < len(driver["orders"]):
        order = driver["orders"][j]
        if order["id"] == order_id:
            #User picked up the order
            if order_type == "S":
                order["status"] = 12
                j+=1
            #User delieveried the order
            elif order_type == "C" and order["status"] == 12:
                driver["orders"].pop(j)
            return
        else:
            j+=1

def check_available(driver, t):
    """Check if driver is on duty"""
    return driver["start_time"] <= t <= driver["end_time"]

def global_id_to_unix_time(global_id):
    """Convert global_id to unix time"""
    d = datetime.datetime.strptime(global_id,"%Y%m%d-%H%M%S%f")
    unix_time = int(d.timestamp())
    return unix_time

def get_inputs(date,city_id):
    inputs_name = "lambda_inputs"
    current_list = os.listdir(".")
    if inputs_name not in current_list:
        os.mkdir("./"+inputs_name)
    file_name = date + "-" + str(city_id) + ".json"
    lambda_input_files = os.listdir("./lambda_inputs/")
    if file_name in lambda_input_files:
        with open("./lambda_inputs/" + file_name, "r") as f:
            online_inputs = json.loads(f.read())
        return online_inputs
    else:
        input_jsons = get_global_id.fetch_from_s3bucket(date + "0" * 12, city_id)
        input_jsons.sort(key=lambda val: val["global_id"])
        online_inputs = []
        for input_data in input_jsons:
            input_data["unixtime"] = global_id_to_unix_time(input_data["global_id"])
            try:
                input_data.pop("EC2")
                online_inputs.append(input_data)
            except:
                pass
        with open("./lambda_inputs/" + file_name, "w") as f:
            f.write(json.dumps(online_inputs))
    return online_inputs

def checkDate(s):
    if s <= 9:
        return "0" + str(s)
    else:
        return str(s)

if __name__ == "__main__":
    print(locator_helper.get_trip_time_distance([43.7604155, -79.4112158], [43.760373, -79.411115]))