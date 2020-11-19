import json
import os
import subprocess
import boto3
import datetime
import time
import requests
import sys
import math
from math import sin, cos, sqrt, atan2, radians
from pandas.io.json import json_normalize
import tzlocal
import pandas as pd
from pytz import timezone
from copy import deepcopy


def upload_file_bucket(file_name, bucket_name):
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket_name, file_name)
    s3_object.upload_file('/tmp/' + file_name)


def count_file_lines(fname):
    num_lines = 0
    with open(fname, 'r') as f:
        for line in f:
            num_lines += 1
    return num_lines


def cal_distance(lat1, lng1, lat2, lng2):
    R = 6373.0
    lat1 = radians(float(lat1))
    lon1 = radians(float(lng1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(lng2))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return sqrt(2) * R * c


def cal_min_interval(c):
    os.environ['TZ'] = 'US/Eastern'
    time.tzset()
    eastern = timezone('US/Eastern')
    local_timezone = tzlocal.get_localzone()
    order_hour = datetime.datetime.fromtimestamp(c, local_timezone).hour
    if order_hour < 8:
        prev_date = (datetime.datetime.today() - datetime.timedelta(days=1)).date()
        benchmark = str(prev_date) + " 08:00:00"
        benchmark = datetime.datetime.strptime(benchmark, '%Y-%m-%d %H:%M:%S')
        diff = datetime.datetime.fromtimestamp(c, local_timezone) - eastern.localize(benchmark)
        time_interval = int((diff.total_seconds()) // 60)
    else:
        curr_date = datetime.datetime.today().date()
        benchmark = str(curr_date) + " 08:00:00"
        benchmark = datetime.datetime.strptime(benchmark, '%Y-%m-%d %H:%M:%S')
        diff = datetime.datetime.fromtimestamp(c, local_timezone) - eastern.localize(benchmark)
        time_interval = int((diff.total_seconds()) // 60)
    return time_interval


def lambda_handler(event, context):
    ''''
    #receive city_id 
    content = event["body"]
    print(content)
    city_id =event["queryStringParameters"]
    print(city_id)
    '''

    url_driver = "https://admin.foodhwy.com/api/qindom/driver/list"
    url_order = "https://admin.foodhwy.com/api/qindom/order/list"
    headers = {"Qindom-Token": "EpyzcsGnUvAqXpgyuE6QUKymsEExV5Pj"}

    # get order with status with 10 through API as json
    params = {'status': '10', 'order_by': 'asc'}
    orderinfo = requests.get(url_order, headers=headers, params=params).json()
    driverinfo = requests.get(url_driver, headers=headers).json()
    # print(orderinfo)

    # change driver info from json to dataframe and drop duplicate
    driverTable = json_normalize(driverinfo["data"])
    driverTable = driverTable.drop_duplicates(subset='aid', keep='first')

    # filter orders with status 10
    orderTable = json_normalize(orderinfo["data"])
    if orderTable.empty:
        return {"body": json.dumps("no orders!")}
    unCheck = orderTable.loc[orderTable["status"] == 10]

    regions = orderTable.city_id.unique()
    print(unCheck)

    for cols in unCheck.columns:
        if '0' in unCheck[cols].tolist():
            return {"body": json.dumps("no data in " + str(cols) + " ! ")}

    # create dat file for each region
    for i in regions:
        all_pos_info = []
        pos_cate = []
        regions_driver = driverTable.loc[driverTable["city_id"] == i]
        # print(regions_driver.to_json(orient='index'))
        if regions_driver.empty:
            return {"body": json.dumps("no drivers in this region!")}

        region_driver = regions_driver["aid"]
        region_driver_num = len(region_driver)
        driver_pos = [['0', '0']] + [[row['lat'], row['lng']] for index, row in regions_driver.iterrows()]
        aids_orders = [[row['aid'], row['orders']] for index, row in regions_driver.iterrows()]
        # print(region_driver_num)

        regions_order = orderTable.loc[orderTable["city_id"] == i]
        print(regions_order.to_json(orient='index'))
        demands_new = [[row['id'], row['cookingtime_set']] for index, row in regions_order.iterrows()]
        customer_new = [[row['customer_id'], row['shop_id']] for index, row in regions_order.iterrows()]
        shop_pos_new = [[row['shop_lat'], row['shop_lng']] for index, row in regions_order.iterrows()]
        cus_pos_new = [[row['customer_lat'], row['customer_lng']] for index, row in regions_order.iterrows()]
        date_time = [cal_min_interval(c) for c in regions_order["created_at"]]

        # create demand in dat and record their pos info
        demand = "Demands={ " + "\n" + "<0 0 0 0 0 0 0>," + "\n"
        for j in range(region_driver_num):
            demand = demand + "<" + str(j + 1) + ' ' + str(region_driver[j]) + " 0 0 0 0 0>," + "\n"
            all_pos_info.append({str(aids_orders[j][0]): driver_pos[j + 1], "category": "D"})

        # create pref and truck in dat
        pref = "Pref={ " + "\n"
        truck = "Trucks={ " + "\n"
        k = 0
        for g in aids_orders:
            driver_id = g[0]
            # if driver has no order in hands, add new order's created_at time to truck
            if not g[1]:
                truck = truck + "<" + str(k) + ' ' + str(driver_id) + ' ' + str(k + 1) + " 0 500 " + str(
                    date_time[0]) + ' ' + str(date_time[0] + 180) + ">," + "\n"
            # if driver has order in hands, add earliest order's created_at time to truck
            else:
                create_time = []
                for j in g[1]:
                    order_id = j['id']
                    pref = pref + "<" + str(order_id) + ' ' + str(driver_id) + " 1>," + "\n"
                    create_time.append(j['created_at'])
                create_time = list(map(lambda x: cal_min_interval(x), create_time))
                truck = truck + "<" + str(k) + ' ' + str(driver_id) + ' ' + str(k + 1) + " 0 500 " + str(
                    min(create_time)) + ' ' + str(min(create_time) + 180) + ">," + "\n"
            k = k + 1
        pref = pref + "};" + "\n \n"
        truck = truck + "};" + "\n \n"
        # print(pref)
        # print(truck)

        demands_new_num = len(demands_new)
        for m in range(demands_new_num):
            demand = demand + ''.join(["<" + str(region_driver_num + m + 1) + ' ' + str(customer_new[m][1]) + ' ' + str(
                demands_new[m][0]) + ' ' + str(date_time[m]) + ' ' + str(date_time[m] + 180) + ' ' + str(
                demands_new[m][1] // 60) + " 1>," + "\n"])
            all_pos_info.append({str(customer_new[m][0]): cus_pos_new[m], "category": "NC"})
            all_pos_info.append({str(customer_new[m][1]): shop_pos_new[m], "category": "NS"})

        regions_driver = regions_driver["orders"]
        demands_old = [[row['id'], row['cookingtime_set']] for a in regions_driver for index, row in
                       pd.DataFrame(a).iterrows()]
        customer_old = [[row['customer_id'], row['shop_id']] for a in regions_driver for index, row in
                        pd.DataFrame(a).iterrows()]
        shop_pos_old = [[row['shop_lat'], row['shop_lng']] for a in regions_driver for index, row in
                        pd.DataFrame(a).iterrows()]
        cus_pos_old = [[row['customer_lat'], row['customer_lng']] for a in regions_driver for index, row in
                       pd.DataFrame(a).iterrows()]
        time_old = [cal_min_interval(t["created_at"]) for k in regions_driver for t in k]
        # print(time_old)

        demands_old_num = len(demands_old)
        demand = demand + ''.join(["<" + str(region_driver_num + demands_new_num + m + 1) + ' ' + str(
            customer_old[m][1]) + ' ' + str(demands_old[m][0]) + ' ' + str(time_old[m]) + ' ' + str(
            time_old[m] + 180) + ' ' + str(demands_old[m][1] // 60) + " 1>," + "\n" for m in range(demands_old_num)])
        demand = demand + ''.join(["<" + str(region_driver_num + demands_new_num + demands_old_num + n + 1) + ' ' + str(
            customer_new[n][0]) + ' ' + str(demands_new[n][0]) + ' ' + str(date_time[n]) + ' ' + str(
            date_time[n] + 180) + ' ' + " 0 0>," + "\n" for n in range(demands_new_num)])

        for n in range(demands_old_num):
            demand = demand + ''.join(["<" + str(
                region_driver_num + 2 * demands_new_num + demands_old_num + n + 1) + ' ' + str(
                customer_old[n][0]) + ' ' + str(demands_old[n][0]) + ' ' + str(time_old[n]) + ' ' + str(
                time_old[n] + 180) + ' ' + " 0 0>," + "\n"])
            all_pos_info.append({str(customer_old[n][0]): cus_pos_old[n], "category": "C"})
            all_pos_info.append({str(customer_old[n][1]): shop_pos_old[n], "category": "S"})
        demand = demand + "};" + "\n \n"
        # print(demand)
        # print(all_pos_info)

        # create dist in dat
        dist = "Dists={ " + "\n"
        line_num = 1 + region_driver_num + 2 * demands_new_num + 2 * demands_old_num
        shop_pos = shop_pos_new + shop_pos_old
        shop_num = len(shop_pos)
        cus_pos = cus_pos_new + cus_pos_old
        cus_num = len(cus_pos)
        region_driver_num = region_driver_num + 1
        for m in range(line_num):
            for n in range(line_num):
                if n == 0 or m == 0:
                    dist = dist + "<" + str(m) + ' ' + str(n) + " 0>," + "\n"
                elif n < region_driver_num and m < region_driver_num:
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(driver_pos[m][0], driver_pos[m][1], driver_pos[n][0],
                                     driver_pos[n][1]))) + ">," + "\n"
                elif m < region_driver_num and region_driver_num <= n < (region_driver_num + shop_num):
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(driver_pos[m][0], driver_pos[m][1], shop_pos[n - region_driver_num][0],
                                     shop_pos[n - region_driver_num][1]))) + ">," + "\n"
                elif m < region_driver_num and (region_driver_num + shop_num) <= n < line_num:
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(driver_pos[m][0], driver_pos[m][1], cus_pos[n - region_driver_num - shop_num][0],
                                     cus_pos[n - region_driver_num - shop_num][1]))) + ">," + "\n"
                elif n < region_driver_num and region_driver_num <= m < (region_driver_num + shop_num):
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(shop_pos[m - region_driver_num][0], shop_pos[m - region_driver_num][1],
                                     driver_pos[n][0], driver_pos[n][1]))) + ">," + "\n"
                elif n < region_driver_num and (region_driver_num + shop_num) <= m < line_num:
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(cus_pos[m - region_driver_num - shop_num][0],
                                     cus_pos[m - region_driver_num - shop_num][1], driver_pos[n][0],
                                     driver_pos[n][1]))) + ">," + "\n"
                elif region_driver_num <= n < (region_driver_num + shop_num) and region_driver_num <= m < (
                        region_driver_num + shop_num):
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(shop_pos[m - region_driver_num][0], shop_pos[m - region_driver_num][1],
                                     shop_pos[n - region_driver_num][0],
                                     shop_pos[n - region_driver_num][1]))) + ">," + "\n"
                elif (region_driver_num + shop_num) <= n <= line_num and region_driver_num <= m < (
                        region_driver_num + shop_num):
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(shop_pos[m - region_driver_num][0], shop_pos[m - region_driver_num][1],
                                     cus_pos[n - region_driver_num - shop_num][0],
                                     cus_pos[n - region_driver_num - shop_num][1]))) + ">," + "\n"
                elif (region_driver_num + shop_num) <= m <= line_num and region_driver_num <= n < (
                        region_driver_num + shop_num):
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(cus_pos[m - region_driver_num - cus_num][0],
                                     cus_pos[m - region_driver_num - cus_num][1], shop_pos[n - region_driver_num][0],
                                     shop_pos[n - region_driver_num][1]))) + ">," + "\n"
                else:
                    dist = dist + "<" + str(m) + ' ' + str(n) + ' ' + str(3 * math.ceil(
                        cal_distance(cus_pos[m - region_driver_num - cus_num][0],
                                     cus_pos[m - region_driver_num - cus_num][1],
                                     cus_pos[n - region_driver_num - cus_num][0],
                                     cus_pos[n - region_driver_num - cus_num][1]))) + ">," + "\n"
        dist = dist + "};" + "\n \n"
        # print(dist)

        optpara = "OptParams={ \n <timeLimit, 15>, \n};\n"
        dat = demand + dist + truck + "Obj={ \n<0 travel_time 1 1>, \n<1 load_balance 1 500>, \n};" + "\n" + optpara + pref
        # print(dat)
        with open('/tmp/datafile.dat', 'w') as f:
            f.write(dat)
        with open('/tmp/datafile.dat', 'r') as f:
            dat = f.readlines()

        # throw dat to opl 5 times to get a driver list
        # start_rwfile_time = datetime.datetime.now()
        path = os.environ['LAMBDA_TASK_ROOT']
        driverList = []
        for i in range(3):
            proc = subprocess.Popen([path + '/oplrun', 'foodhwy/foodhwy_multi_pickup_API.mod', '/tmp/datafile.dat'],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = proc.communicate()
            data = out.decode('utf-8')
            data_err = err.decode('utf-8')
            # print(data)
            # print(data_err)

            # clean extra info in return
            sol_start = data.find('**')
            pairs = {}
            if (sol_start == -1):
                message = "We don't have optimal solution for this case"
            else:
                message = []
                dirt_message = []
                sol_end = data.find('<<< post process')
                opt_sol = data[sol_start:sol_end].splitlines()
                for k in opt_sol:
                    dirt_message = dirt_message + k.split('**')
                dirt_message = list(filter(None, dirt_message))
                for k in range(len(dirt_message) // 2):
                    message.append({"order_id": dirt_message[2 * k], "deliverer_id": dirt_message[2 * k + 1]})
                pairs = message
            driverList.append(message)
            # end_rwfile_time = datetime.datetime.now()

            # get the route for drivers
            routes_start = data.find('+++')
            routes_end = data.find('**')
            routes_en = data[routes_start:routes_end].splitlines()
            routes = []
            for i in routes_en:
                routes.append(list(filter(None, i.split('+++'))))
                # print(routes)
            route_line = deepcopy(routes)
            routes_len = len(routes)

            new_route_len = 0
            if isinstance(driverList[0][0], dict):
                new_driver = driverList[0][0]["deliverer_id"]
                for k in range(routes_len):
                    for j in range(len(routes[k])):
                        for n in range(len(all_pos_info)):
                            if isinstance(routes[k][j], str) and routes[k][j] in (all_pos_info[n]).keys():
                                if routes[k][j] == str(new_driver):
                                    # new_route_len = len(routes[k])
                                    pos_cate.append("ND")
                                else:
                                    pos_cate.append(all_pos_info[n]["category"])
                                routes[k][j] = all_pos_info[n][routes[k][j]]
                                # print("routes: \n"+ str(routes))
            # print(pos_cate)

            lat = [[] for i in range(routes_len)]
            lon = [[] for i in range(routes_len)]
            c = 0;
            for k in routes:
                for j in k:
                    lat[c].append(float(j[0]))
                    lon[c].append(float(j[1]))
                c = c + 1

            # add additional pref to produce driver list
            new_order_id = demands_new[0][0]
            dat_len = len(dat)
            for m in pairs:
                if new_order_id == int(m["order_id"]):
                    pre_driver_id = int(m["deliverer_id"])
                dat.insert(dat_len - 2, "<" + str(new_order_id) + ' ' + str(pre_driver_id) + " 0>," + "\n")
            with open('/tmp/datafile.dat', 'w+') as f:
                for i in range(dat_len):
                    f.write(dat[i])

            file_name = str(new_order_id) + '.log'
            content = str(dat) + '\n' + str(route_line) + '\n' + str(pos_cate) + '\n' + str(lat) + '\n' + str(
                lon) + '\n' + str(driverList) + '\n' + str(data) + '\n' + str(data_err) + '\n'
            with open('/tmp/' + file_name, 'w+') as f:
                f.write(content)
            upload_file_bucket(file_name, 'rongxie-test')

            # driverList.append(lat)
            # driverList.append(lon)
            # driverList.append(pos_cate)
            print(driverList)
            return {"body": json.dumps(driverList)}
