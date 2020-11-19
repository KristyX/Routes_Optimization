# -*- coding:latin-1 -*-
import json
import pandas as pd
import numpy as np
import datetime
import itertools
import os
from foodhwy_errors import *
from utility import load_region_hist, upload_file_use_firehose, save_region_hist, log_error
from schema import schema
from jsonschema import Draft7Validator
from constants import *
from pprint import pprint
import requests
import hashlib


def save_input_data_to_s3(req, UniqueTimeId):
    try:
        # req['global_id'] = UniqueTimeId
        upload_file_use_firehose(FIREHOSE_MSG, json.dumps(req))
    except Exception:
        log_error(300, "upload request data to firehose error", UniqueTimeId)
        pass


def check_schema_error(req):
    v = Draft7Validator(schema)
    errors = sorted(v.iter_errors(req), key=lambda e: e.path)
    return errors


def validate_sync_data(req):
    if "syncdata" in req.keys():
        if req["syncdata"] == "true":
            # Load existing syncdata and add the new data
            syncdata = load_region_hist('syncdata', 'list')
            save_region_hist('syncdata', req["data"])
            return True
        else:
            return False
    else:
        return False


def validate_health_check(req):
    if "orderinfo" in req.keys():
        if req["orderinfo"] == "Health Check":
            return True
        else:
            return False
    else:
        return False


def update_orders(dict_assign, dict_order, list_driverinfo):
    for key, value in dict_assign.items():
        for i1, order in enumerate(dict_order):
            for i2, driver in enumerate(list_driverinfo):
                if driver['aid'] == value and order['id'] == key:
                    # print(driver['orders'])
                    list_driverinfo[i2]['orders'].append(order)
    return list_driverinfo


def remove_assigned_orders(dict_assign, dict_order, all_assigned_orders):
    # Remove the orders from dict_assign that have already been assigned to a driver
    if all_assigned_orders is not None:
        for idx, oid in enumerate(all_assigned_orders):
            dict_assign.pop(oid, None)

    # Remove the orders from dict_order
    remove_list = list(dict_assign.keys())
    # print("remove_list:", remove_list)
    dict_order = [item for item in dict_order if item['id'] in remove_list]

    return dict_assign, dict_order


def preprocess(req):
    city_id = req["city_id"]
    driverinfo = req["driverinfo"]["data"]
    orderinfo = req["orderinfo"]["data"]
    params_path = ENV_PATH + "params.json"
    # user can reset params if they has password
    if "password" in req and req["password"] == hashlib.md5(b"Luci_foodhwy").hexdigest():
        if "reset" in req and req["reset"] == True and os.path.exists(params_path):
            os.remove(params_path)
    if not os.path.exists(params_path):
        params = {}
        params["optpara"] = {
            "timeLimit": OPT_TIME_LIMIT,
            "solutionLimit": SOL_LIMIT,
            "heuristic_num": HEUR_NUM,
            "model_path":  CITY_MODEL_MAP[city_id]
        }
        params["objection"] = {'driving_time_weight': 2, 'tips_weight': 1, 'waiting_time_weight': 0}
    else:
        with open(params_path, "r") as f:
            params = json.loads(f.read())
    if "optpara" in req:
        for key in req["optpara"]:
            if key in params["optpara"]:
                params["optpara"][key] = req["optpara"][key]
    if "objection" in req:
        params["objection"] = req["objection"]
    if 'rejection' in req:
        reject_objs = req['rejection']
    else:
        reject_objs = []

    logistics_params = {"city_id": city_id,
                        "driverinfo": driverinfo,
                        "orderinfo": orderinfo,
                        "rejection": reject_objs,
                        "objection": params["objection"],
                        "optpara": params["optpara"],
                        "current_time": int(
                            datetime.datetime.strptime(req["global_id"], "%Y%m%d-%H%M%S%f").timestamp()),
                        }
    # user can overwrite params if they have password
    if "password" in req and req["password"] == hashlib.md5(b"Luci_foodhwy").hexdigest():
        with open(params_path, "w") as f:
            f.write(json.dumps(params))
    return logistics_params


def cal_distance(loc1, loc2):
    R = 6373.0
    loc1_rad = np.radians(loc1)
    loc2_rad = np.radians(loc2)
    d = loc2_rad - loc1_rad
    sind = np.sin(d / 2) ** 2
    a = sind[..., 0] + np.cos(loc1_rad[..., 0]) * np.cos(loc2_rad[..., 0]) * sind[..., 1]
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return np.sqrt(2) * R * c


def get_multiple_osrm_triptime(points):
    return calculate_driving_time(points, points)


def _calculate_driving_time(sources, destinations):
    location_str = ";".join(map(lambda x: str(x[1]) + "," + str(x[0]), sources))
    sources_idx = ";".join(map(str, list(range(len(sources)))))
    location_str += ";" + ";".join(map(lambda x: str(x[1]) + "," + str(x[0]), destinations))
    destinations_idx = ";".join(map(str, list(range(len(sources), len(sources) + len(destinations)))))
    url = "http://3.222.89.226:80/table/v1/driving/{}?sources={}&destinations={}".format(location_str,
                                                                                         sources_idx,
                                                                                         destinations_idx)
    r = requests.post(url)
    assert (r.status_code == 200)
    data = json.loads(r.text)
    duration = np.array(data["durations"])
    travel_time = np.around(np.array(duration / 60))
    return travel_time


def calculate_driving_time(sources, destinations):
    """Return a 2D list of travel """
    travel_time = []
    for i in range(0, len(sources), 100):
        i_row = []
        for j in range(0, len(destinations), 100):
            i_sources = sources[i:i + 100]
            j_destinations = destinations[j:j + 100]
            i_j_travel_time = _calculate_driving_time(i_sources, j_destinations)
            i_row.append(i_j_travel_time)
        i_row = np.concatenate(i_row, axis=1)
        travel_time.append(i_row)
    travel_time = np.concatenate(travel_time, axis=0)
    return travel_time


def format_distance_dat(distance_matrix):
    row, col = distance_matrix.shape
    dist = "Dists={ \n"
    for i in range(row):
        for j in range(col):
            dist += "<" + str(i) + " " + str(j) + " " + str(distance_matrix[i, j]) + ">,\n"
    dist += "};\n \n"
    return dist


def format_dat(Matrix, name):
    res = name + "={ \n"
    for row in Matrix:
        res += "<" + (" ".join(row.astype(str))) + ">,\n"
    res += "};\n \n"
    return res


def format_obj_dat(obj):
    return "Obj={ \n <0 driving_time_weight 1 " + str(obj["driving_time_weight"]) + \
           ">, \n <1 waiting_time_weight 1 " + str(obj["waiting_time_weight"]) + \
           ">, \n <2 tips_weight 1 " + str(obj["tips_weight"]) + \
           ">, \n};\n\n"


def format_optpara_dat(OptParams):
    return "OptParams={ \n  <timeLimit " + str(OptParams["timeLimit"]) + ">, \n <solutionLimit " + str(
        OptParams["solutionLimit"]) + ">\n };\n\n"


def add_id(matrix):
    key_id = np.arange(matrix.shape[0]).reshape(-1, 1)
    res = np.concatenate((key_id, matrix), axis=1).astype(int)
    return res


def update_Pref(Pref, locations, set_value):
    current_locations = Pref[:, :2]
    # set Pref in locations to set_value
    Pref[:, 2][(current_locations[:, None] == locations).all(2).any(1)] = set_value
    # add new_locations to pref
    new_locations = locations[~(locations[:, None] == current_locations).all(2).any(1)]
    new_locations = np.concatenate((new_locations, np.zeros((new_locations.shape[0], 1)) + set_value), axis=1)
    Pref = np.concatenate((Pref, new_locations), axis=0)
    return Pref.astype(int)


def prams2DataFrame(logistics_params):
    # regionDriversTable
    driverTable = pd.DataFrame(logistics_params["driverinfo"])
    newOrderTable = pd.DataFrame(logistics_params["orderinfo"])
    newOrderTable["cookingtime_set"] = newOrderTable["cookingtime_set"] * 60
    if "req_time" not in newOrderTable:
        newOrderTable["req_time"] = logistics_params["current_time"]
    if driverTable.empty:
        raise NoDriverError("No available driver provided!")
    if newOrderTable.empty:
        raise NoOrderError("No new order provided!")
    regionDriversTable = driverTable.loc[driverTable["city_id"] == logistics_params["city_id"]]
    regionDriversTable = regionDriversTable[regionDriversTable.aid.notnull()]
    if regionDriversTable.empty:
        raise NoDriverError("No available driver provided!")
    key_id = np.arange(1, regionDriversTable.shape[0] + 1).reshape(-1, 1)
    regionDriversTable["idx"] = key_id
    if "start_time" not in regionDriversTable:
        regionDriversTable["start_time"] = logistics_params["current_time"]
    if "end_time" not in regionDriversTable:
        regionDriversTable["end_time"] = logistics_params["current_time"]
    regionNewOrdersTable = newOrderTable.loc[newOrderTable["city_id"] == logistics_params["city_id"]]
    driverOrdersTable = []
    for driverOrder in regionDriversTable[["idx", "aid", "orders"]].values:
        if driverOrder[2] == []:
            orders = pd.DataFrame(driverOrder[2], columns=["shop_lat", "shop_lng", "customer_lat", "customer_lng"])
        else:
            orders = pd.DataFrame(driverOrder[2])
        orders["idx"] = driverOrder[0]
        orders["aid"] = driverOrder[1]
        driverOrdersTable.append(orders)
    driverOrdersTable = pd.concat(driverOrdersTable, sort=False).reset_index(drop=True)
    reject = [{"order_id": new_order[0], "idx": driver[0]} for new_order in newOrderTable[["id", "req_time"]].values for
              driver in regionDriversTable[["idx", "end_time"]].values if new_order[1] > driver[1]]
    rejectTable = pd.DataFrame(reject, columns=["order_id", "idx"])  # this will ignore original rejections
    res = {"driver_table": regionDriversTable,
           "driver_order_table": driverOrdersTable,
           "new_order_table": regionNewOrdersTable,
           "reject_table": rejectTable, }
    return res


def nearest_driver_heuristic(tables, driver_num):
    driverTable, driverOrderTable, newOrderTable, reject = tables["driver_table"], tables["driver_order_table"], tables[
        "new_order_table"], tables["reject_table"]
    centers = pd.DataFrame(np.concatenate([driverTable[["aid", "lat", "lng"]].values,
                                           driverOrderTable[["aid", "shop_lat", "shop_lng"]].values,
                                           driverOrderTable[["aid", "customer_lat", "customer_lng"]].values,
                                           ], axis=0), columns=["aid", "lat", "lng"])
    centers[["lat", "lng"]] = centers[["lat", "lng"]].astype(float)
    centers = centers.groupby("aid").mean()
    distances = cal_distance(centers[["lat", "lng"]].values.reshape(-1, 1, 2),
                             newOrderTable[["shop_lat", "shop_lng"]].values.astype(float))
    driverNewOrderDistance = np.concatenate([centers.index.values.reshape(-1, 1), distances], axis=1)
    nearest_driver = driverNewOrderDistance[driverNewOrderDistance[:, 1].argsort()][:driver_num][:, 0]
    tables["driver_table"] = driverTable[driverTable["aid"].isin(nearest_driver)]
    tables["driver_order_table"] = driverOrderTable[driverOrderTable["aid"].isin(nearest_driver)]
    tables["reject_table"] = reject[reject["driver_id"].isin(nearest_driver)]
    return tables


def driverDemands(driverTable):
    position_id = driverTable[["aid"]].values
    default = np.zeros(position_id.shape)
    return np.concatenate((position_id,
                           default,
                           default,
                           default,
                           default,
                           default,
                           default,
                           default), axis=1)


def shopDemands(orderTable, current_time):
    shopTable = orderTable[orderTable["status"] != 12]
    s = shopTable[["shop_id"]].values.shape
    default = np.zeros(s)
    min_time = shopTable[["created_at"]] - current_time
    max_time = min_time.values + 3600 * 8
    min_time = np.around(min_time.values / 60)
    max_time = np.around(max_time / 60)
    cookingtime_set = np.around((shopTable[["cookingtime_result"]].values - shopTable[["created_at"]].values) / 60)
    shop_demands = np.concatenate((shopTable[["shop_id", "id"]].values, min_time, max_time,
                                   cookingtime_set,
                                   default,
                                   default, default), axis=1)
    return shop_demands


def customerDemands(orderTable, current_time):
    s = orderTable[["customer_id"]].values.shape
    defult = np.zeros(s)
    est_time = defult + CUSTOMER_WAITING_TIME
    quantity = np.ones(s)
    distance = orderTable[["distance"]].values
    late_time = vfunc_estimate_late_time(distance)
    min_time = orderTable[["created_at"]] - current_time
    max_time = min_time.values + 3600 * 8
    min_time = np.around(min_time.values / 60)
    max_time = np.around(max_time / 60)
    customer_demands = np.concatenate((orderTable[["customer_id", "id"]].values, min_time, max_time,
                                       defult,
                                       est_time,
                                       quantity,
                                       late_time), axis=1)
    return customer_demands


def estimate_late_time(distance):
    if distance <= 3000:
        return 35
    elif distance <= 5000:
        return 45
    elif distance <= 10000:
        return 70
    else:
        return 85


vfunc_estimate_late_time = np.vectorize(estimate_late_time)  # move it to somewhere better


def cartesian_product(a, b):
    return np.array(list(itertools.product(a, b))).reshape(-1, a.shape[1] + b.shape[1])


def produce_data(logistics_params):
    '''
    In this function, we want to produce all data that optimize function needs to run
    '''
    # tables = nearest_driver_heuristic(prams2DataFrame(logistics_params), logistics_params["optpara"]["heuristic_num"])
    tables = prams2DataFrame(logistics_params)
    # We need to produce:
    # Demands
    regionDriversTable, driverOrdersTable, regionNewOrdersTable, reject = tables["driver_table"], tables[
        "driver_order_table"], tables["new_order_table"], tables["reject_table"]
    Demands = [[np.zeros((1, 8))], [], []]
    Demands_types = [["N"], [], []]
    Trucks = pd.DataFrame()
    Pref = np.empty((0, 3))
    points = [[], [], []]
    created_at_time = []
    if regionDriversTable.empty == False:
        # Demands
        driver_demands = driverDemands(regionDriversTable)
        Demands[0].append(driverDemands(regionDriversTable))
        Demands_types[0] += len(driver_demands) * ["D"]
        # Trucks
        Trucks["aid"] = regionDriversTable["aid"].values
        Trucks["first_visit_id"] = regionDriversTable["idx"].values
        Trucks["last_visit_id"] = np.zeros(Trucks["aid"].shape)
        Trucks["capacity"] = np.zeros(Trucks["aid"].shape) + 500
        Trucks["start_time"] = regionDriversTable["start_time"].values - logistics_params["current_time"]
        Trucks["end_time"] = Trucks["start_time"] + 3600 * 8
        # Trucks["last_call"] = regionDriversTable["end_time"] - logistics_params["current_time"]
        Trucks = Trucks[["aid", "first_visit_id", "last_visit_id", "capacity", "start_time", "end_time"]].values
        Trucks[:, 4:] = np.around(Trucks[:, 4:] / 60)
        Trucks = add_id(Trucks)
        Trucks = Trucks.astype(int)
        # Distance
        points[0].append(regionDriversTable[["lat", "lng"]].values.astype(float))
    else:
        raise NoDriverError("No available driver provided!")
    if regionNewOrdersTable.empty == False:
        # Demands
        shop_demands = shopDemands(regionNewOrdersTable, logistics_params["current_time"])
        customer_demands = customerDemands(regionNewOrdersTable, logistics_params["current_time"])
        Demands[1].append(shop_demands)
        Demands_types[1] += len(shop_demands) * ["NS"]
        Demands[2].append(customer_demands)
        Demands_types[2] += len(customer_demands) * ["NC"]
        # Distance
        points[1].append(regionNewOrdersTable[['shop_lat', 'shop_lng']].values.astype(float))
        points[2].append(regionNewOrdersTable[['customer_lat', 'customer_lng']].values.astype(float))
        created_at_time += regionNewOrdersTable["created_at"].values.tolist()
    else:
        raise NoOrderError("No new order provided!")
    if driverOrdersTable.empty == False:
        # Demands
        shop_demands = shopDemands(driverOrdersTable, logistics_params["current_time"])
        customer_demands = customerDemands(driverOrdersTable, logistics_params["current_time"])

        Demands[1].append(shop_demands)
        Demands_types[1] += len(shop_demands) * ["S"]
        Demands[2].append(customer_demands)
        Demands_types[2] += len(customer_demands) * ["C"]
        # Pref under driverOrdersTable is not empty
        Pref = update_Pref(Pref, driverOrdersTable[["id", "aid"]].values, 1)
        # Distance
        shopLocation = driverOrdersTable[driverOrdersTable.status != 12][["shop_lat", "shop_lng"]]
        points[1].append(shopLocation[['shop_lat', 'shop_lng']].values.astype(float))
        points[2].append(driverOrdersTable[['customer_lat', 'customer_lng']].values.astype(float))
        created_at_time += driverOrdersTable["created_at"].values.tolist()
    if reject.empty == False:
        Pref = update_Pref(Pref, reject[["order_id", "idx"]].values, 0)

    # Demands
    # print(OrderDemands.astype(int))
    Demands = np.concatenate(Demands[0] + Demands[1] + Demands[2], axis=0)
    Demands = add_id(Demands)
    Demands = Demands.astype(int)
    Demands = np.concatenate([Demands, np.array(Demands_types[0] + Demands_types[1] + Demands_types[2]).reshape(-1, 1)],
                             axis=1)
    # Distance
    points = np.concatenate(points[0] + points[1] + points[2], axis=0)
    travel_time = get_multiple_osrm_triptime(points)
    if len(travel_time[np.logical_or(travel_time < 0, travel_time > 1440)]) > 0:
        raise TravelTimeError("Locations are too seperate!")
    travel_time = np.concatenate((np.zeros((1, travel_time.shape[1])), travel_time), axis=0)
    travel_time = np.concatenate((np.zeros((travel_time.shape[0], 1)), travel_time), axis=1).astype(int)
    # Pref
    Pref = Pref.astype(int)
    res = {"demands": Demands,
           "trucks": Trucks,
           "distance": travel_time,
           "pref": Pref,
           "obj": logistics_params["objection"],
           "optpara": logistics_params["optpara"],
           "dataframes": tables,
           "current_time": logistics_params["current_time"],
           }
    # print("****Print dat***********:")
    res["dat"] = generate_dat(res)
    # pprint(res["dat"])
    return res


def generate_dat(data):
    dat = format_dat(data["demands"], "Demands") + \
          format_dat(data["trucks"], "Trucks") + \
          format_distance_dat(data["distance"]) + \
          format_obj_dat(data["obj"]) + \
          format_optpara_dat(data["optpara"])
    return dat
