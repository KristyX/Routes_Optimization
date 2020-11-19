import requests
import time
import numpy as np
import json
import boto3
import os
from foodhwy_fake_data import *
from foodhwy_real_data import *
from description_generator import *
from trace import *
from get_json import *
from pprint import pprint
import itertools
from request_methods import *

def init_order(order_opt):
    if order_opt["option"] == "random":
        return np.random.randint(order_opt["low"], order_opt["high"], size=order_opt["driver_num"]).tolist()
    elif order_opt["option"] == "same":
        return [order_opt["init_num"] for i in range(order_opt["driver_num"])]
    elif order_opt["option"] == "skew":
        sknum = int(order_opt["driver_num"]*order_opt["skew_rate"])
        return [order_opt["init_num"] for i in range(sknum)] + [0 for i in range(order_opt["driver_num"]-sknum)]

def generate_driver_info_opt(driver_num, order_opt, city_id):
    order_opt["driver_num"] = driver_num
    return {"driver_num":driver_num,"city_id":city_id,"driver_orders":init_order(order_opt)}

def generate_order_info_opt(order_num, city_id):
    return {"order_num":order_num,"city_ids":[city_id for i in range(order_num)]}

def check_result(result):
    solved = 0
    input_error = 0
    server_error = 0
    time_out = 0
    try:
        if result["status"] == 200:
                solved = 1
        elif result["status"] == 400:
            input_error = 1
        elif result["status"] == 500:
            server_error = 1
    except:
        time_out = 1
    return solved, input_error, server_error, time_out

def lambda_test(url, message, driver_num, order_opt, city_id, order_num, iterate_time):
    headers = {"x-api-key": "D1mHMAoU4u76E8tnItABc8R3WnpufFHr7PZpSu5g"}
    solved_total_time = 0
    total_time = 0
    total_solved = 0
    total_input_error = 0
    total_server_error = 0
    total_time_out = 0
    print("*********************************************")
    for i in range(iterate_time):
        driver_info_opt = generate_driver_info_opt(driver_num, order_opt,city_id)
        order_info_opt = generate_order_info_opt(order_num, city_id)
        message["driverinfo"],message["orderinfo"] = generate_data(driver_info_opt, order_info_opt, i)
        message_json = json.dumps(message)
        input_path = "./input_jsons/"+str(driver_num)+"_"+str(order_num)+"_"+str(i)+".json"
        with open(input_path,"w+") as f:
            f.write(message_json)
        consume_time, result = my_request(url, headers, message_json, "POST")
        print("TEST",i+1,"result:")
        print(result)
        try:
            print(respond2description(result))
        except:
            print("Unable to interperate")
        print("time cost:",consume_time)
        if result["status"] != 200:
            try:
                json_trace(message_json,str(driver_num)+"_"+str(order_num)+"_"+str(i)+".dat")
                print("trace success!")
            except:
                pass
        print("++++++++++++++++++++++++++++++++++++++++++")
        total_time+=consume_time
        solved, input_error, server_error, time_out = check_result(result)
        if solved == 1:
            solved_total_time += consume_time
        total_solved += solved
        total_input_error += input_error
        total_server_error += server_error
        total_time_out += time_out
    print("In summary:")
    print("driver_num: "+str(driver_num)+ " order_num: "+str(order_num))
    print("tests number: "+str(iterate_time)+ " average time consumed: "+ str(total_time/iterate_time))
    print("solved num:",total_solved, "solved total time:",solved_total_time,"solved percentage:",total_solved/iterate_time)
    print("input_error num: ",total_input_error)
    print("server_error num: ",total_server_error)
    print("time out num: ",total_time_out)
    print("*******************************")
    return solved_total_time, total_time, total_solved, total_input_error, total_server_error, total_time_out, total_time/iterate_time

def pressure_test(city_id,driver_lower, driver_upper, order_lower, order_upper,order_opt,url,driver_jump,order_jump, iterate_time):
    optpara={'timeLimit': 1,"heuristic_num":10, "solutionLimit":5}
    message = {"optpara": optpara, "city_id": city_id}

    driver_nums = [i for i in range(driver_lower,driver_upper+1,driver_jump)]
    order_nums = [j for j in range(order_lower ,order_upper+1,order_jump)]
    for driver_num in driver_nums:
        for order_num in order_nums:
            lambda_test(url, message, driver_num, order_opt, city_id, order_num, iterate_time)

def branchmark_test(branchmark_path, mod_path, params=None):
    branchmark = pd.read_csv(branchmark_path)
    global_ids = branchmark["global_id"].values
    best_driver_ids = branchmark["best_driver_id"].values
    good_driver_ids = branchmark["good_driver_id"].values
    perfect_num = 0
    accept_num = 0
    not_perfect = []
    not_acceptable = []
    for global_id, best_driver_id, good_driver_id in zip(global_ids, best_driver_ids, good_driver_ids):
        input_assignment, sync_assignment = global_id_trace(global_id, mod_path, params)
        suggest = input_assignment[0]
        if suggest == int(best_driver_id):
            perfect_num += 1
        else:
            not_perfect.append(global_id)
            if str(suggest) in str(good_driver_id):
                accept_num += 1
            else:
                not_acceptable.append(global_id)
    print("Result:")
    pprint(params)
    print("number of correctness:", perfect_num, "out of", len(global_ids))
    print("Not perfect global_ids:", not_perfect)
    print("number of acceptable:", accept_num + perfect_num, "out of", len(global_ids))
    print("Not acceptable global_ids:", not_acceptable)
    return perfect_num, accept_num + perfect_num

def grid_search(grid):
    best_config = None
    max_acceptable_num = 0
    l = [grid[key] for key in grid]
    for i in itertools.product(*l):
        params = dict(zip(grid,i))
        perfect_num, acceptable_num = branchmark_test("branchmark.csv", "./lambda_env/foodhwy2.mod", {"objection":params})
        if acceptable_num > max_acceptable_num:
            best_config = params
            max_acceptable_num = acceptable_num
    return best_config, max_acceptable_num

def real_data_test(path,startDate, endDate, notCheckList=[]):
    #url = "https://api.luci.ai/multipickup"
    #foodhwy-1 short name -1
    url = "https://9wfqctz4i8.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    #foodhwy
    url = "https://pq6u9ao5ij.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    #url = "https://api.luci.ai/multipickup"
    headers = {"x-api-key": "D1mHMAoU4u76E8tnItABc8R3WnpufFHr7PZpSu5g"}
    before = []
    failed = 0
    while 1:
        #time.sleep (10)
        after = read_jsons(path,startDate,endDate)
        print("start please wait")
        for message in after:
            req = json.loads(message)
            if message not in before and "city_id" in req and req["city_id"] not in notCheckList:
                consume_time, result = my_request(url, headers, message, "POST")
                print(result)
                if result["message"] == "Internal server error":
                    failed+=1
                    with open("./input_jsons/test"+str(failed)+".json", "w+") as f:
                        f.write(message)
                        print("saved")
                elif result["status"] != 200:
                    failed+=1
        before = after
        print("finished one round")
        return

def jsons_test(url, headers, path):
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f)) and not f.startswith(".")]
    with open(path, "r") as f:
        req = json.loads(f.read())
        consume_time, result = my_request(url, headers, json.dumps(req), "POST")
        print(result)

def failed_jsons_test(input_folder_path,output_folder_path,trace):
    json_files = [f for f in os.listdir(input_folder_path) if isfile(join(input_folder_path, f)) and not f.startswith(".")]
    for file_name in json_files:
        print("...processing:",file_name)
        trace(input_folder_path+file_name,output_folder_path)
        print("successed:",file_name)

def list_compare(l1,l2):
    l1 = np.array(l1)
    l2 = np.array(l2)
    print(l1[~np.isin(l1, l2)])
    print(l2[~np.isin(l2, l1)])

def online_benchmark(benchmark_path, url):
    benchmark = pd.read_csv(benchmark_path)
    global_ids = benchmark["global_id"].values
    best_driver_ids = benchmark["best_driver_id"].values
    good_driver_ids = benchmark["good_driver_id"].values
    perfect_num = 0
    accept_num = 0
    not_perfect = []
    not_acceptable = []
    for global_id, best_driver_id, good_driver_id in zip(global_ids, best_driver_ids, good_driver_ids):
        input_assignment= global_id_online_trace(global_id, url)
        suggest = input_assignment[0]
        if suggest == best_driver_id:
            perfect_num += 1
        else:
            not_perfect.append(global_id)
            if str(suggest) in str(good_driver_id):
                accept_num += 1
            else:
                not_acceptable.append(global_id)
    print("Result:")
    print("number of correctness:", perfect_num, "out of", len(global_ids))
    print("Not perfect global_ids:", not_perfect)
    print("number of acceptable:", accept_num + perfect_num, "out of", len(global_ids))
    print("Not acceptable global_ids:", not_acceptable)
    return perfect_num, accept_num + perfect_num

if __name__ == "__main__":
    #global_id_trace("20190902-115049698916","./lambda_env/foodhwy2.mod")
    grid = {"driving_time_weight":[1,2,4,8],
    "waiting_time_weight":[0,1,4,8],
    "tips_weight":[0,1,2,4,8]
    }
    #"delivery_speed":[2/6,3/6,4/6,5/6]}
    #best_config, max_acceptable_num = grid_search(grid)
    #branchmark_test("branchmark.csv", "./lambda_env/foodhwy2.mod",{"objection":best_config})
    #branchmark_test("benchmark817-822.csv", "./lambda_env/foodhwy2.mod")
    #branchmark_test("benchmark830-901.csv", "./lambda_env/foodhwy2.mod")

    #params = {'driving_time_weight': 1, 'load_balance': 1, 'tips_weight': 1}
    #branchmark_test("branchmark.csv", "./lambda_env/foodhwy2.mod", {"objection":params})
    #global_id_trace("20190905-110528626515","./lambda_env/foodhwy2.mod")
    '''
    l = ["20190902-115159698512","20190902-115049698916","20190902-115417762935","20190902-122256410192"]
    for i in l:
        global_id_trace(i,"./lambda_env/foodhwy2.mod")
    '''
    '''
    l1 = ['20190817-164335413630', '20190817-170753222787', '20190817-171237794461', '20190817-171826507478', '20190817-181810992017', '20190817-183023218142', '20190817-184556886984', '20190822-115135073724', '20190822-121021453970', '20190822-124026659862']
    l2 = ['20190817-164335413630', '20190817-170753222787', '20190817-171237794461', '20190817-171826507478', '20190817-181810992017', '20190817-183023218142', '20190817-184556886984', '20190822-115135073724', '20190822-121021453970', '20190822-124026659862']
    list_compare(l1,l2)
    '''
    '''
    real_data_test("./data_info/", 20190815,None)
    '''
    real_data_test("./data_info/",20190901, 20190902)
    #foodhwy-1
    #url = "https://9wfqctz4i8.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    #foodhwy
    #url = "https://pq6u9ao5ij.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    #url = "https://api.luci.ai/multipickup"
    #online_benchmark("benchmark817-822.csv", url)
    #online_benchmark("benchmark830-901.csv", url)

    #failed_jsons_test("./input_jsons/","./output_jsons/", json2output_trace)

    '''
    index = 0
    while True:
        index = real_data_test(index)
    '''

    '''
    driver_num = 40
    url = "https://fkhpu3p8db.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    #url = "http://127.0.0.1:3000/multipickup"
    #url = "https://bcjw45lnib.execute-api.us-west-1.amazonaws.com/Prod/multipickup"
    order_opt = {"low": 5,
                 "high": 6,
                 "option":"random", #same/skew/random
                 "init_num": 5, #use it when option is same/skew
                 "skew_rate": 0.3}
    pressure_test_opt = {"driver_lower":driver_num,
                         "driver_upper":driver_num,
                         "order_lower":1,
                         "order_upper":1,
                         "order_opt":order_opt,
                         "url":url,
                         "driver_jump":10,
                         "order_jump":1,
                         "iterate_time":10,}
    pressure_test(**pressure_test_opt)
    #driver_info_opt= generate_driver_info_opt(24, order_opt)
    #print(generate_driver_info(driver_info_opt))
    #dat_test()
    '''
