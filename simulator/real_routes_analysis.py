import pymysql
import datetime
import time
from pprint import pprint
import pandas as pd
import util
import evaluation
import locator_helper
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def convert_date_to_unixtime(year, month, day):
    d = datetime.date(year, month, day)
    unixtime = time.mktime(d.timetuple())
    return unixtime


def foodhwy_sql_query(year, month, day, city_id):
    db = pymysql.connect('xxx', 'xxx', 'xxx',
                         'xxx')
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
    """return all available orders"""
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


def lookup_convertor(inputs):
    lookup = {}
    orders = []
    for input in inputs:
        for order in input["orderinfo"]["data"]:
            drivers = {}
            orders.append(order)
            for driver in input["driverinfo"]["data"]:
                drivers[driver["aid"]] = driver
            drivers["req_time"] = util.global_id_to_unix_time(input["global_id"])
            lookup[order["id"]] = drivers
    return lookup, pd.DataFrame(orders)


def real_aggregate_routes(date, city_id):
    online_input = util.get_inputs(date, city_id)
    lookup, new_order_table = lookup_convertor(online_input)
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])
    real_data_file_name = str(date) + "-" + str(city_id) + ".csv"
    database_folder = "./database/"
    if not os.path.exists(database_folder):
        os.makedirs(database_folder)
    database_folder_files = os.listdir(database_folder)
    if real_data_file_name in database_folder_files:
        df = pd.read_csv(database_folder + real_data_file_name)
    else:
        df = foodhwy_sql_query(year, month, day, city_id)
        df = get_standard_dos(df)
        with open(database_folder + real_data_file_name, "w") as f:
            f.write(df.to_csv(index=False))
    res = {}
    driver_orders = {}
    driver_prev = {}
    driver_assign_table = []
    for row in df.values:
        driver_id = row[0]
        order_id = row[1]
        status = row[2]
        # driver accept order
        if status == 15:
            driver = lookup[order_id][row[0]]
            driver_assign_table.append({"id": order_id,
                                        "driver_id": driver["aid"],
                                        "driver_lat": driver["lat"],
                                        "driver_lng": driver["lng"]})
            if row[0] not in res:
                res[row[0]] = []
                driver_orders[row[0]] = 0
            if driver_orders[row[0]] == 0:
                assignment = {"driver_id": row[0], "route_plan": [], "driver_location": [driver["lat"], driver["lng"]],
                              "driver_start_time": lookup[row[1]]["req_time"]}
                res[row[0]].append(assignment)
                driver_prev[driver_id] = {"prev_loc": assignment["driver_location"],
                                          "prev_time": assignment["driver_start_time"]}
            driver_orders[driver_id] += 1
        else:
            o = new_order_table[new_order_table["id"] == order_id]
            # The order has been picked up
            if status == 12:
                drop_by_id = int(o[["shop_id"]].values[0])
                loc = [row[4], row[5]]
                drop_by_type = "S"
            # The order has been handed to a customer
            else:
                drop_by_id = int(o[["customer_id"]].values[0])
                loc = [row[6], row[7]]
                drop_by_type = "C"
                driver_orders[driver_id] -= 1

            travel_time, distance = locator_helper.get_trip_time_distance(driver_prev[driver_id]["prev_loc"], loc)
            cur_time = travel_time + driver_prev[driver_id]["prev_time"]
            place = {"drop_by_id": drop_by_id, "drop_by": loc, "order_id": order_id,
                     "drop_by_start_time": cur_time, "drop_by_end_time": row[3], "drop_by_type": drop_by_type,
                     "distance": distance}
            res[driver_id][-1]["route_plan"].append(place)
            driver_prev[driver_id] = {"prev_loc": loc,
                                      "prev_time": place["drop_by_end_time"]}
    driver_assign_table = pd.DataFrame(driver_assign_table)
    driver_order_table = pd.merge(driver_assign_table, new_order_table, on=["id"])
    driver_order_table.shipping_amount = driver_order_table.shipping_amount.astype(float)
    return res, driver_order_table


if __name__ == "__main__":
    '''
    city_id = 3149
    date = "20190926"
    aggregate_routes, driver_order_table = real_aggregate_routes(date, city_id)
    # s = time.time()
    # simulator_aggregate_routes = util.simulator_aggregate_routes_calculator(aggregate_routes, driver_order_table)
    import json
    driver_orders_folders = "./driver_orders"
    if not os.path.exists(driver_orders_folders):
        os.makedirs(driver_orders_folders)
    with open("./driver_orders/"+str(city_id)+"-"+date, "w") as f:
        f.write(driver_order_table.to_csv(index=False))
    # aggregate_routes_folder = "./aggregate_routes"
    # if not os.path.exists(aggregate_routes_folder):
    #     os.makedirs(aggregate_routes_folder)
    # with open("./aggregate_routes/real.json", "w") as f:
    #     f.write(json.dumps(aggregate_routes))
    # with open("./aggregate_routes/fake.json", "w") as f:
    #     f.write(json.dumps(simulator_aggregate_routes))

    with open("./aggregate_routes/real.json", "r") as f:
        aggregate_routes = json.loads(f.read())
    with open("./aggregate_routes/fake.json", "r") as f:
        simulator_aggregate_routes = json.loads(f.read())
    print(aggregate_routes)
    print(simulator_aggregate_routes)
    '''
    import mysql.connector

    mysql_cn = mysql.connector.connect(host='192.168.0.128', port=3306,
                                       user='foodhwy', passwd='raptors2019', db='foodhwy_pro')
    luci_orders_view = 'select * from request_driverinfo_from_s3;'
    view = pd.read_sql(luci_orders_view, con=mysql_cn)
    with open("./luci_orders_log_view.csv","w") as f:
        f.write(view.to_csv(index=False))
    print(view)
