import pymysql
import requests
import datetime
import time
import pytz
from pprint import pprint
import pandas as pd
import os
from paramiko import SSHClient
from scp import SCPClient
import json
import numpy as np
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings("ignore")


def foodhwy_sql_query(year, month, day, city_id):
    db = pymysql.connect()
    cursor = db.cursor()
    # TODO: day may conflict
    unixtime1 = convert_date_to_unixtime(year, month, day)
    unixtime2 = convert_date_to_unixtime(year, month, day + 1)
    sql = "SELECT `operator_id`, `order_id`, `new_order_status`, `updated_at` FROM luci_orders_log_view " \
          "WHERE city_id = %s AND updated_at >= %s and updated_at < %s " \
          "AND (`new_order_status` = 15 OR `new_order_status`= 12 OR `new_order_status`= 20)"
    cursor.execute(sql, (city_id, unixtime1, unixtime2))
    result = list(cursor.fetchall())
    result = list(map(lambda x: list(x), result))
    df_result = pd.DataFrame.from_records(result, columns=["driver_id", "order_id", "status", "unixtime"])
    df_result.sort_values(by=['unixtime'], ascending=True, inplace=True)
    df_result.index = range(len(df_result))

    sql_orderinfo = "SELECT `order_id`, `shop_lat`, `shop_lng`, `customer_lat`, `customer_lng`, `distance`, " \
                    "`driver_shipping_amount` FROM luci_orders_view WHERE `city_id`=%s"
    # AND `created_at` >=%s and `created_at` < %s
    cursor.execute(sql_orderinfo, (city_id))  # , unixtime1, unixtime2
    order_result = list(cursor.fetchall())
    order_result = list(map(lambda x: list(x), order_result))
    df_order_result = pd.DataFrame.from_records(order_result,
                                                columns=["order_id", "shop_lat", "shop_lng", "customer_lat",
                                                         "customer_lng", "distance", "driver_shipping_amount"])
    df_merge = df_result.merge(df_order_result, left_on="order_id", right_on="order_id", how="left")
    df_zero = df_merge[df_merge["driver_id"] == 0][["driver_id", "order_id"]]
    if len(df_zero) != 0:
        df_main = df_merge[df_merge["driver_id"] != 0][["driver_id", "order_id"]]
        df_zero_merge = df_zero.reset_index().merge(df_main, left_on="order_id", right_on="order_id",
                                                    how="inner").set_index('index')
        df_zero_merge.drop_duplicates(inplace=True)
        df_zero_merge = df_zero_merge[["order_id", "driver_id_y"]]
        for index, row in df_zero_merge.iterrows():
            df_merge.at[index, "driver_id"] = row["driver_id_y"]
    df_merge = get_standard_dos(df_merge)
    return df_merge


def get_standard_dos(df):
    order_list = list(df["order_id"].values)
    bad_order_list = []
    for order in order_list:
        if order_list.count(order) != 3:
            bad_order_list.append(order)
    bad_order_list = list(set(bad_order_list))
    print("bad order list: ", bad_order_list)
    df = df[~df["order_id"].isin(bad_order_list)]
    print("delete rows: ", len(order_list) - len(df))
    return df


def luci_sql_query(year, month, day):
    db = pymysql.connect('192.168.0.128', 'foodhwy', 'raptors2019', 'foodhwy_pro')
    cursor = db.cursor()
    sql = "SELECT * FROM driver_order_status WHERE timestamp >= %s AND timestamp < %s AND status = 15"
    # TODO: may conflict
    timestamp1 = convert_date_to_timestamp(year, month, day)
    timestamp2 = convert_date_to_timestamp(year, month, day + 1)
    cursor.execute(sql, (timestamp1, timestamp2))
    result = list(cursor.fetchall())
    result = list(map(lambda x: list(x), result))
    df_result = pd.DataFrame.from_records(result,
                                          columns=["driver_id", "driver_lat", "driver_lng", "order_id", "status",
                                                   "timestamp"])
    return df_result


def get_str_dt(int_dt):
    if int_dt < 10:
        return str(0) + str(int_dt)
    else:
        return str(int_dt)


def convert_date_to_timestamp(year, month, day):
    year_str = get_str_dt(year)
    month_str = get_str_dt(month)
    day_str = get_str_dt(day)
    timestamp = str(year_str) + str(month_str) + str(day_str) + "000000000000"
    return timestamp


def convert_unixtime_to_datetime(unixtime):
    date_time = datetime.datetime.fromtimestamp(unixtime).strftime("%Y%m%d%H%M%S")
    return date_time


def convert_date_to_unixtime(year, month, day):
    d = datetime.date(year, month, day)
    unixtime = time.mktime(d.timetuple())
    return unixtime


def fetch_sync_json_from_gpu(year, month, day):
    # os.remove("/var/luci/foodhwy/data_file/sync_json/*")
    year_str = get_str_dt(year)
    month_str = get_str_dt(month)
    day_str = get_str_dt(day)
    timestamp = year_str + month_str + day_str + "*"
    remote_path = "/var/luci/foodhwy/data_info/" + str(timestamp)
    local_path = "/var/luci/foodhwy/data_file/sync_json/"
    print(remote_path)
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect("192.168.0.128", username="raptors", password="raptors2019")
    with SCPClient(ssh.get_transport(), sanitize=lambda x: x) as scp:
        scp.get(remote_path, local_path)


def get_driver_location(json_path):
    driver_location = {}
    json_list = os.listdir(json_path)
    for file_name in json_list:
        file = open(json_path + file_name, "r")
        json_content = json.load(file)
        timestamp = json_content["timestamp"][:14]
        driverinfo = json_content["dirverinfo"]
        driver_location[timestamp] = {}
        for driver in driverinfo:
            driver_id = driver["aid"]
            driver_lat = driver["lat"]
            driver_lng = driver["lng"]
            driver_location[timestamp][driver_id] = [driver_lat, driver_lng]
    return driver_location


def get_time_range(timestamp, diff_minute):
    date_time = datetime.datetime.strptime(timestamp, '%Y%m%d%H%M%S')
    min_time = date_time - datetime.timedelta(minutes=diff_minute + 3)
    max_time = date_time + datetime.timedelta(minutes=diff_minute)

    min_timestamp = min_time.strftime("%Y%m%d%H%M%S")
    max_timestamp = max_time.strftime("%Y%m%d%H%M%S")

    return min_timestamp, max_timestamp


def generate_driver_location_by_timestamp(df_dos, driver_time_loc, diff_minute):
    timestamp_list = list(driver_time_loc.keys())
    df_dos["location"] = None
    for index, row in df_dos.iterrows():
        if row["status"] == 12:
            df_dos.at[index, "location"] = [str(row["shop_lat"]), str(row["shop_lng"])]
        elif row["status"] == 20:
            df_dos.at[index, "location"] = [str(row["customer_lat"]), str(row["customer_lng"])]
        elif row["status"] == 15:
            timestamp = convert_unixtime_to_datetime(row["unixtime"])
            if (timestamp in timestamp_list) and row["driver_id"] in list(driver_time_loc[timestamp].keys()):
                df_dos.at[index, "location"] = driver_time_loc[timestamp][row["driver_id"]]
            else:
                min_timestamp, max_timestamp = get_time_range(timestamp, diff_minute)
                min_diff = diff_minute * 100
                for ts in timestamp_list:
                    if (ts >= min_timestamp) and (ts <= max_timestamp) and (row["driver_id"] in list(
                            driver_time_loc[ts].keys())):
                        diff = abs(int(timestamp) - int(ts))
                        if diff < min_diff:
                            min_diff = diff
                            target_ts = ts
                try:
                    df_dos.at[index, "location"] = driver_time_loc[target_ts][row["driver_id"]]
                except:
                    pass
    null_order_id = list(df_dos[df_dos.isnull().any(axis=1)]["order_id"].values)
    print("null order id: ")
    print(null_order_id)
    df = df_dos[~df_dos["order_id"].isin(null_order_id)][
        ["driver_id", "order_id", "status", "unixtime", "location", "distance", "driver_shipping_amount"]]
    df.index = range(len(df))
    return df


def get_stat_for_each_driver(df, driver_list):
    # route_dic = {}
    dist_dic = {}
    for driver in driver_list:
        df_driver = df[df["driver_id"] == driver]
        df_driver.sort_values("unixtime", ascending=True, inplace=True)
        # generate routes
        route_list = []
        dist_driver = 0
        charged_dist = 0
        shipping_fee = 0
        df_route = pd.DataFrame(
            columns=["driver_id", "order_id", "status", "unixtime", "location", "distance", "driver_shipping_amount"])
        order_tmp_list = []
        num_order = 0
        for index, row in df_driver.iterrows():
            df_route = df_route.append(row, ignore_index=True)
            order_id = row["order_id"]
            order_tmp_list.append(order_id)
            if order_tmp_list.count(order_id) == 3:
                order_tmp_list = list(filter(lambda x: x != order_id, order_tmp_list))
                # Charged distance
                charged_dist += float(
                    df_route[(df_route["order_id"] == order_id) & (df_route["status"] == 20)]["distance"])
                # Shipping fee
                shipping_fee += float(
                    df_route[(df_route["order_id"] == order_id) & (df_route["status"] == 20)]["driver_shipping_amount"])
                # Number of orders
                num_order += 1
                if len(order_tmp_list) == 0:
                    # route_list.append(df_route)
                    # Total distance
                    dist_route = calculate_route_distance(df_route)
                    dist_driver += dist_route
                    df_route = pd.DataFrame(
                        columns=["driver_id", "order_id", "status", "unixtime", "location", "distance",
                                 "driver_shipping_amount"])
        if route_list is []:
            print("Backlogged orders occured!!", driver)
        # route_dic[driver] = route_list
        dist_dic[driver] = [dist_driver, charged_dist, shipping_fee, num_order]
    df_stat = pd.DataFrame.from_dict(dist_dic, orient="index",
                                     columns=["total_distance", "charged_distance", "shipping_fee", "num_orders"])
    return df_stat


def generate_report(df_stat):
    df_stat["dist_unit_profit"] = df_stat.apply(lambda x: x["charged_distance"] / x["total_distance"], axis=1)
    return


def get_timestmp_from_json_name(file_name):
    return int(file_name[:-5])


def get_distance(loc1, loc2):
    globals = {'true': 0, 'false': 1}
    if loc1 == loc2:
        return 0.0
    r = requests.post("http://3.222.89.226:80/route/v1/driving/{},{}"
                      ";{},{}?alternatives=false&steps=true&annotations=true&geometries=polyline&overview=full&annotations=true".format(
        loc1[1],
        loc1[0],
        loc2[1], loc2[0]))
    if r.status_code == 200:
        try:
            data = eval(r.text, globals)
            annotations = data['routes'][0]['legs'][0]['annotation']
            # duration = annotations['duration']  # duration in seconds
            # total_travel_time = round(float(sum(duration) / 60), 2)
            distance = annotations['distance']
            total_distance = round(float(sum(distance) / 1000), 2)
            return total_distance
        except Exception:
            return 0.0
    else:
        return 0.0


def calculate_route_distance(df_route):
    dist = 0
    location_list = list(df_route["location"].values)
    for loc_index in range(len(location_list) - 1):
        dist += get_distance(location_list[loc_index], location_list[loc_index + 1])
    return dist


def calculate_driver_profit_flow(year, month, day, city_id, diff_minute,
                                 sync_json_path="/var/luci/foodhwy/data_file/sync_json/"):
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.max_rows', 500)
    df_dos = foodhwy_sql_query(year, month, day, city_id)
    driver_time_loc = get_driver_location(sync_json_path)
    df_status = generate_driver_location_by_timestamp(df_dos, driver_time_loc, diff_minute)
    driver_list = list(set(list(df_status["driver_id"].values)))
    print("Driver list: ", driver_list)
    print(len(driver_list))
    df_stat = get_stat_for_each_driver(df_status, driver_list)
    generate_report(df_stat)
    return df_stat


if __name__ == "__main__":
    diff_minute = 8
    # city_id = 3616
    rds_engine = create_engine(
        'mysql+pymysql://luci_ai:raptors2019@foodhwy-pro.ckexhwoiwcdb.us-east-1.rds.amazonaws.com/foodhwy_rds')
    city_list = [3142, 3149, 3152, 3606, 3609, 3610, 3611, 3612, 3613, 3614, 3615, 3616, 3617, 3618, 3619, 3620,
                 3621] # 3141

    for day in range(29, 32):
        try:
            print(day)
            fetch_sync_json_from_gpu(2019, 9, day)
            for city_id in city_list:
                print(city_id)
                df_stat = calculate_driver_profit_flow(2019, 9, day, city_id, diff_minute)
                df_stat["driver_id"] = df_stat.index
                df_stat["date"] = "2019-09-" + str(day)
                df_stat["city_id"] = city_id
                print(df_stat)
                df_stat.to_sql("mvp_driver_dashboard", con=rds_engine, if_exists="append", index=False)
        except:
            pass
    # df_stat.to_csv("result.csv")
