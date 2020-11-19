import foodhwy_fake_data
import sys
sys.path.append('../lambda_env/main/')
import region3141 as region
import os
DIR = os.getcwd()
OUTER_DIR = "/".join(DIR.split('/')[:-1])
DIR_PATH = OUTER_DIR+'/simulator'
import json
import get_global_id
import util
from SandTable import SandTable
from State import State
from pprint import pprint
from datetime import datetime


def main(environment, action, model_path="../lambda_env/main/sqrt.mod", num_batch=1):
    print("Evaluation start...")
    sand_table = SandTable(environment["driverinfo"], environment["orderinfo"])
    states = []
    for s in range(0,len(sand_table.batches),num_batch):
        print("Calculating state:",s+1)
        batches = sand_table.batches[s:s+num_batch]
        state = State(sand_table, batches)
        # use algorithm to plan the best assignement
        state.apply_action(sand_table, action, model_path)
        states.append(state)
    print("Evaluation End!!")
    return states

def create_environment(date, city_id):
    '''Return a environment with all drivers and orders in given city at given date'''
    environments_name = "environments"
    inputs_name = "lambda_inputs"
    failed_dats = "failed_dats"
    current_list = os.listdir(DIR_PATH)
    if environments_name not in current_list:
        os.mkdir(DIR_PATH+"/"+environments_name)
    if inputs_name not in current_list:
        os.mkdir(DIR_PATH+"/"+inputs_name)
    if failed_dats not in current_list:
        os.mkdir(DIR_PATH + "/" + failed_dats)
    file_name = date + "-" + str(city_id) + ".json"
    input_files = os.listdir(DIR_PATH+"/environments/")
    lambda_input_files = os.listdir(DIR_PATH+"/lambda_inputs/")
    if file_name in input_files:
        with open(DIR_PATH+"/environments/" + file_name, "r") as f:
            environment = json.loads(f.read())
        return environment
    elif file_name in lambda_input_files:
        with open(DIR_PATH+"/lambda_inputs/"+file_name,"r") as f:
            online_inputs = json.loads(f.read())
    else:
        input_jsons = get_global_id.fetch_from_s3bucket(date + "0" * 12, city_id)
        input_jsons.sort(key=lambda val: val["global_id"])
        online_inputs = []
        for input_data in input_jsons:
            input_data["unixtime"] = util.global_id_to_unix_time(input_data["global_id"])
            try:
                input_data.pop("EC2")
                online_inputs.append(input_data)
            except:
                pass
        with open(DIR_PATH+"/lambda_inputs/" + file_name, "w") as f:
            f.write(json.dumps(online_inputs))
    first_global_id = online_inputs[0]["global_id"]
    all_orders = []
    all_drivers = []
    seen = {}
    prev = {}
    for input in online_inputs:
        global_id = input["global_id"]
        unix_time = util.global_id_to_unix_time(global_id)
        drivers = input["driverinfo"]["data"]
        orders = input["orderinfo"]["data"]
        for order in orders:
            order["req_time"] = unix_time
            order["global_id"] = global_id
        all_orders += orders
        cur = {}
        for driver in drivers:
            driver["orders"] = []
            t = {"start_time": unix_time, "end_time": unix_time}
            if driver["aid"] not in seen:
                driver.update(t)
                all_drivers.append(driver)
                driver_to_add = driver
                seen[driver["aid"]] = driver_to_add
            else:
                if driver["aid"] in prev:
                    driver_to_add = prev[driver["aid"]]
                    if driver_to_add["start_time"] > unix_time:
                        driver_to_add["start_time"] = unix_time
                    if driver_to_add["end_time"] < unix_time:
                        driver_to_add["end_time"] = unix_time
                else:
                    driver.update(t)
                    all_drivers.append(driver)
                    driver_to_add = driver
            cur[driver["aid"]] = driver_to_add
        prev = cur
    environment = {"global_id":first_global_id,
    "city_id": city_id,
    "driverinfo":all_drivers,
    "orderinfo": all_orders,}
    with open(DIR_PATH+"/environments/" + file_name, "w") as f:
        f.write(json.dumps(environment))
    return environment

if __name__ == "__main__":
    date = "20190926"
    city_id = 3613  # dt
    city_id = 3149  # yn
    city_id = 3612
    # city_id = 3612 #windsor
    environment = create_environment(date, city_id)
    model_path = "../lambda_env/main/foodhwy/foodhwy_sowt.mod"
    #model_path = "../lambda_env/main/foodhwy/foodhwy_sqrt.mod"
    #model_path = "../lambda_env/main/foodhwy/foodhwy_pwlb3.mod"

    states = main(environment,region.lambda_handler, model_path,num_batch=1)
    state = states[-1]
    print(state.aggregate_routes)
    for aid in state.aggregate_routes:
        routes = state.aggregate_routes[aid]
        for route in routes:
            pprint(route)
            start_location = route["driver_location"]
    #print("driver efficiency", state.drivers_efficiency(environment["orderinfo"]))
    #print("driver orders number per hour", state.drivers_ordernum_per_hour(environment))
