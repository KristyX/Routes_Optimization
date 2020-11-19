import json
import os, shutil
import sys
sys.path.append('./lambda_env')
import preprocess
import opt_algorithm
import postprocess
import get_json
import request_methods
from pprint import pprint
import pandas as pd
import numpy as np
from foodhwy_errors import *

dirpath = os.getcwd()
def produce_dat(data):
    return data["dat"] + preprocess.format_dat(data["pref"], "Pref")


def json2dat(json_obj):
    req = json.loads(json_obj)
    logistics_params = preprocess.preprocess(req)
    data = preprocess.produce_data(logistics_params)
    return produce_dat(data)


def json_file_trace(input_file_path, output_file_path):
    with open(input_file_path, "r") as f:
        dat = json2dat(f.read())
    with open(output_file_path, "w+") as f:
        f.write(dat)


def failed_jsons_trace(input_file_path, output_file_path):
    with open(input_file_path, "r") as f:
        failed_json = f.read()
        req = failed_json[failed_json.find('\n') + 1:]
        dat = json2dat(req)
    with open(output_file_path, "w+") as f:
        print(dat)
        f.write(dat)


def dat2output_trace(data, dat_path, mod_path):
    oplpath = dirpath+"/cplex/oplrun"
    opt_output = opt_algorithm.opt_alg(oplpath, mod_path, dat_path)["opl"]
    result, assignment = postprocess.format_output(data, opt_output)
    return result, assignment


def json2output_trace(req, mod_path, dat_path):
    try:
        logistics_params = preprocess.preprocess(req)
        data = preprocess.produce_data(logistics_params)
        dat = produce_dat(data)
    except NoDriverError:
        return
    except TravelTimeError:
        return
    with open(dat_path, "w+") as f:
        f.write(dat)
        print(dat_path, "saved")
    result, assignment = dat2output_trace(data, dat_path, mod_path)
    return result, assignment


def kristy_input_trace(req):
    input_data = req["input_data"]
    #print(input_data)
    #sync_data = req["sync"]
    sync_data = input_data
    #print(sync_data)
    #sync_data['driverinfo'] = {"data": sync_data['driverinfo']}
    #sync_data['orderinfo'] = {"data": sync_data["orderinfo"]}
    res = req["result"]["results"][0]["opt_plan"]
    res[0].pop("assigned_order_id")
    res[0]["opt_plan"] = res[0]["route_plan"]
    res[0].pop("route_plan")
    return input_data, sync_data, res

def output_folder(mod_path):
    res = mod_path[mod_path.rfind("/")+1:mod_path.rfind(".")] + "/"

    return res

def global_id_online_trace(global_id, url):
    headers = {"x-api-key": "D1mHMAoU4u76E8tnItABc8R3WnpufFHr7PZpSu5g"}
    url_json = get_json.track_flow(global_id)
    req, sync_json, lambda_json = kristy_input_trace(url_json)
    consume_time, res = request_methods.my_request(url, headers, json.dumps(req), "POST")
    return [res['results'][0]["opt_plan"][0]["driver_id"],res['results'][0]["opt_plan"][0]["assigned_order_id"]]

def global_id_trace(global_id, mod_path, params=None):
    path = "./input_jsons/"
    json_name = global_id + ".json"
    if not os.path.exists(path):
        os.mkdir(path)
    input_files = os.listdir(path)
    input_json_path = path + "input_json_" + json_name
    sync_json_path = path + "sync_json_" + json_name
    if ("input_json_" + json_name not in input_files) or ("sync_json_" + json_name not in input_files):
        url_json = get_json.track_flow(global_id)
        input_json, sync_json, lambda_json = kristy_input_trace(url_json)
        with open(input_json_path, "w+") as f:
            f.write(json.dumps(input_json))
            print(input_json_path, "saved")
        with open(sync_json_path, "w+") as f:
            f.write(json.dumps(sync_json))
            print(sync_json_path, "saved")
        '''
        with open("./url_outputs/" + json_name, "w+") as f:
            f.write(json.dumps(lambda_json))
            print("./url_outputs/" + json_name, "saved")
        '''
    else:
        with open(input_json_path, "r") as f:
            input_json = json.loads(f.read())
        with open(sync_json_path, "r") as f:
            sync_json = json.loads(f.read())
    if params != None:
        input_json.update(params)
        sync_json.update(params)
    input_result, input_assignment = json2output_trace(input_json, mod_path, dirpath+"/failed_dats/input_json_" + global_id + ".dat")
    sync_result, sync_assignment = json2output_trace(sync_json, mod_path, dirpath+"/failed_dats/sync_json_" + global_id + ".dat")
    output_path = output_folder(mod_path)
    with open(output_path + "input_json_" + json_name, "w+") as f:
        f.write(json.dumps(input_result))
        print(output_path + "input_json_" + json_name, "saved")
    with open(output_path + "sync_json_" + json_name, "w+") as f:
        f.write(json.dumps(sync_result))
        print(output_path + "sync_json_" + json_name, "saved")
    return input_assignment, sync_assignment


def global_ids_trace(global_ids, mod_path, empty_folders=False):
    paths = ["./input_jsons/", "./failed_dats/", output_folder(mod_path), "./url_outputs/"]
    input_assignments = []
    sync_assignments = []
    if empty_folders:
        for path in paths:
            if os.path.exists(path):
                shutil.rmtree(path)
            os.mkdir(path)
    for global_id in global_ids:
        input_assignment, sync_assignment = global_id_trace(global_id, mod_path)
        input_assignments.append(input_assignment)
        sync_assignments.append(sync_assignment)
    return input_assignments, sync_assignments

if __name__ == "__main__":
    jsons = ["2450426","2432298","2450634","2423265","2426365","2444210"]
    for j in jsons:
        with open("./input_jsons/"+j+".json", "r") as f:
            req = json.loads(f.read())
            result, assignment = json2output_trace(req, "./lambda_env/foodhwy2.mod","./failed_dats/"+j+".dat")
        with open("./foodhwy2/" + j+".json", "w+") as f:
            f.write(json.dumps(result))
            print("./foodhwy2/" + j, "saved")

    #global_id_trace("20190817-183510213503", "./lambda_env/foodhwy2.mod")
    '''
    branchmark = pd.read_csv("branchmark2.csv")
    global_ids = branchmark["global_id"].values
    best_driver_ids = branchmark["best_driver_id"].values
    good_driver_ids = branchmark["good_driver_id"].values
    with open("./mod_list.txt", "r") as f:
        mod_list = f.read().split("\n")
    for mod in mod_list:
        if mod != "":
            input_assignments, sync_assignments = global_ids_trace(global_ids, mod,
                                                                   False)  # if you want to keep previous files, set empty_folders to False
            input_assignments = np.array(input_assignments).astype(int)[:, 0]
            perfect_num = len(best_driver_ids[best_driver_ids == input_assignments])
            not_perfect = global_ids[best_driver_ids != input_assignments].tolist()
            print("number of correctness:", perfect_num, "out of", len(input_assignments))
            print("Not perfect global_ids:", not_perfect)
            accept_num = 0
            for i, j, k in zip(input_assignments, good_driver_ids, global_ids):
                if str(i) in str(j):
                    accept_num += 1
                    not_perfect.remove(k)
            print("number of acceptable:", accept_num + perfect_num, "out of", len(input_assignments))
            print("Not acceptable global_ids:", not_perfect)
    '''

    '''
    driver_num = 5
    order_num = 0
    new_order_num = 1
    file_name = "fake_" + str(driver_num) + "_" + str(order_num) + "_" + str(new_order_num)
    with open("./input_jsons/"+file_name+ ".json","r") as f:
        req = json.loads(f.read())
        print(req)
        result, assignment = json2output_trace(req,"./foodhwy1.mod","./failed_dats/"+file_name+".dat")
        print(assignment)
    with open("./foodhwy/"+file_name+ ".json", "w+") as f:
        f.write(json.dumps(result))
        print("./foodhwy/"+file_name+ ".json","saved")
    '''
