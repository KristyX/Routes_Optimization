import json
import pymysql
import pandas as pd
from config import *
from pprint import pprint


def fetch_from_s3(latest_global_id, bucket, prefix):
    latest_timestamp = latest_global_id.replace("-", "")
    response = s3client.list_objects(Bucket=bucket, Prefix=prefix)
    target_json_list = []
    if "Contents" in response:
        for file in response['Contents']:
            try:
                fname = file["Key"]
                obj = s3.Object(bucket, fname)
                target = obj.get()['Body'].read().decode('utf-8')
                if target != {}:
                    new_data = target.replace('}{', '},{')
                    json_data_list = json.loads(f'[{new_data}]')
                    for target_json in json_data_list:
                        globalid_time = str(target_json["global_id"]).replace("-", "")
                        if globalid_time > latest_timestamp:
                            # global_id = target_json["global_id"]
                            target_json_list.append(target_json)
            except:
                pass
    return target_json_list


def convert_request_structure(target_json, order_list):
    '''
    :param target_json:
    :return:
    df_order; df_driver
    unit:
    km; min
    '''
    global_id = target_json["global_id"]
    print(global_id)
    # __________________________________________________________________________________________
    # Construct order information
    city_id = target_json["city_id"]
    order_id = target_json["orderinfo"]["data"][0]["id"]
    order_list.append(order_id)
    # if ("source" in target_json) and (target_json["source"]=="foodhwy"):
    if "source" in target_json:
        source = target_json["source"]
    else:
        source = "undefined"

    if ("charged" in target_json) and (target_json["charged"] == "True"):
        charged = "True"
    else:
        charged = "False"

    order_info = target_json["orderinfo"]["data"][0]
    created_at = order_info["created_at"]
    cookingtime_set = order_info["cookingtime_set"]
    cookingtime_result = order_info["cookingtime_result"]
    customer_id = order_info["customer_id"]
    customer_lat = order_info["customer_lat"]
    customer_lng = order_info["customer_lng"]
    distance = float(order_info["distance"]) / 1000
    shop_id = order_info["shop_id"]
    shipping_amount = order_info["shipping_amount"]
    subtotal = order_info["subtotal"]
    shop_lat = order_info["shop_lat"]
    shop_lng = order_info["shop_lng"]
    shop_tel = order_info["shop_tel"]
    shop_addr = order_info["shop_addr"]

    driver_info = target_json["driverinfo"]["data"]
    num_drivers = len(driver_info)
    print(num_drivers)
    order_dic = {"global_id": [global_id], "order_id": [order_id], "city_id": [city_id], "num_drivers": [num_drivers],
                 "source": [source], "charged": [charged], "created_at": [created_at],
                 "cookingtime_set": [cookingtime_set], "cookingtime_result": [cookingtime_result],
                 "customer_id": [customer_id], "customer_lat": [customer_lat], "customer_lng": [customer_lng],
                 "distance": [distance], "shop_id": [shop_id], "shipping_amount": [shipping_amount],
                 "subtotal": [subtotal], "shop_lat": [shop_lat], "shop_lng": [shop_lng], "shop_tel": [shop_tel],
                 "shop_addr": [shop_addr]}
    df_order = pd.DataFrame.from_dict(order_dic)
    # __________________________________________________________________________________________
    # Construct driver information for this order
    df_driver = pd.DataFrame([])
    i = 0
    if num_drivers != 0:
        for driver in driver_info:
            driver_dic = {"global_id": [global_id], "driver_id": [driver["aid"]], "driver_lat": [driver["lat"]],
                          "driver_lng": [driver["lng"]]}
            if "nickname" in driver:
                driver_dic["nickname"] = driver["nickname"]
            else:
                driver_dic["nickname"] = "undefined"
            if "driver_lv" in driver:
                driver_dic["driver_lv"] = driver["driver_lv"]
            else:
                driver_dic["driver_lv"] = -1
            orders = driver["orders"]
            # ___________________________________________________________________________________
            # Construct order information under each driver at this time
            if len(orders) != 0:
                for order in orders:
                    # ___________________________________________________________________________
                    # New order first occured in driverinfo instead of previous orderinfo
                    if order["id"] not in order_list:
                        fake_id = global_id[:-6] + str((100000 + i))
                        i += 1
                        order_insert_dic = {"global_id": [fake_id], "order_id": [order["id"]],
                                            "city_id": [order["city_id"]],
                                            "num_drivers": [0],
                                            "source": ["insert"], "charged": ["False"],
                                            "created_at": [order["created_at"]],
                                            "cookingtime_set": [float(order["cookingtime_set"]) / 60],
                                            "cookingtime_result": [order["cookingtime_result"]],
                                            "customer_id": [order["customer_id"]],
                                            "customer_lat": [order["customer_lat"]],
                                            "customer_lng": [order["customer_lng"]],
                                            "distance": [float(order["distance"]) / 1000],
                                            "shop_id": [order["shop_id"]],
                                            "shipping_amount": [order["shipping_amount"]],
                                            "subtotal": [order["subtotal"]], "shop_lat": [order["shop_lat"]],
                                            "shop_lng": [order["shop_lng"]],
                                            "shop_tel": [order["shop_tel"]],
                                            "shop_addr": [order["shop_addr"]]}
                        order_list.append(order["id"])
                        df_insert_tmp = pd.DataFrame.from_dict(order_insert_dic)
                        df_order = df_order.append(df_insert_tmp)

                    driver_dic["uniq_id"] = global_id + "-" + str(driver["aid"]) + "-" + str(order["id"])
                    driver_dic["order_id"] = order["id"]
                    driver_dic["status"] = order["status"]
                    driver_dic["city_id"] = order["city_id"]
                    driver_dic["cookingtime_result"] = order["cookingtime_result"]
            else:
                driver_dic["uniq_id"] = global_id + "-" + str(driver["aid"])
            df_tmp_driver = pd.DataFrame.from_dict(driver_dic)
            df_driver = df_driver.append(df_tmp_driver)
    pprint(df_driver)
    pprint(df_order)
    return df_order, df_driver, order_list


def get_orders_from_mysql():
    order_list = []
    try:
        sql = "SELECT order_id FROM foodhwy_pro.request_order_from_s3 ORDER BY global_id DESC"  # LIMIT 3000
        fhw_cursor.execute(sql)
        order_list = list(map(lambda x: x[0], fhw_cursor.fetchall()))
    except:
        pass
    return order_list


def insert_into_mysql(self, sql, val):
    self.cursor.execute(sql, val)
    self.db.commit()
    return


def get_prefix_list(latest_global_id, bucket, root_prefix):
    prefix_list = []
    nouse_prefix = []
    response = s3client.list_objects(Bucket=bucket, Prefix=root_prefix, Delimiter='/')
    if "CommonPrefixes" in response:
        prefix_list = [p["Prefix"] for p in response["CommonPrefixes"]]
    for prefix in prefix_list:
        split_prefix = prefix.split("/")
        if (split_prefix[2] == "") and (int(split_prefix[1][5:7]) < int(latest_global_id[4:6])):
            nouse_prefix.append(prefix)
        elif (int(split_prefix[1][5:7]) == int(latest_global_id[4:6])) and ("day" in split_prefix[2]):
            if int(split_prefix[2][3:5]) < int(latest_global_id[6:8]):
                nouse_prefix.append(prefix)
        prefix_list = sorted(list(set(prefix_list) - set(nouse_prefix)))
    return prefix_list


def fetch_convert_save_request_flow():
    # __________________________________________________________________________________________
    # get all the directories need to be scan, and where to start
    latest_global_id = get_latest_global_id()
    prefix_list = get_prefix_list(latest_global_id, foodhwy_bucket, origin_request_prefix)
    for month_prefix in prefix_list:
        date_prefix_list = get_prefix_list(latest_global_id, foodhwy_bucket, month_prefix)
        for date_prefix in date_prefix_list:
            order_list = get_orders_from_mysql()
            df_order_date = pd.DataFrame([])
            df_driver_date = pd.DataFrame([])
            print(date_prefix)
            request_json_list = fetch_from_s3(latest_global_id, foodhwy_bucket, date_prefix)
            for target_json in request_json_list:
                try:
                    df_order, df_driver, order_list = convert_request_structure(target_json, order_list)
                    df_order_date = df_order_date.append(df_order)
                    df_driver_date = df_driver_date.append(df_driver)
                except:
                    pass
            try:
                if len(df_order_date) != 0:
                    df_order_date.drop_duplicates(subset="global_id", keep="first", inplace=True)
                    df_order_date.drop_duplicates(subset="order_id", keep="first", inplace=True)
                    print(df_order_date)
                    df_order_date.to_sql("request_order_from_s3", con=fhw_engine, if_exists="append", index=False)
            except:
                pass
            try:
                if len(df_driver_date) != 0:
                    print(df_driver_date)
                    df_driver_date.to_sql("request_driverinfo_from_s3", con=fhw_engine, if_exists="append", index=False)
            except:
                pass


def get_latest_global_id():
    sql = "SELECT global_id FROM foodhwy_pro.request_order_from_s3 ORDER BY global_id DESC"
    fhw_cursor.execute(sql)
    latest_global_id = fhw_cursor.fetchone()[0]
    print(latest_global_id)

    # sql = "SELECT global_id FROM foodhwy_pro.request_driverinfo_from_s3 ORDER BY global_id DESC"
    # fhw_cursor.execute(sql)
    # latest_global_id = fhw_cursor.fetchone()[0]
    # print(latest_global_id)

    return latest_global_id


if __name__ == "__main__":
    saveto_request_file_path = "/var/luci/foodhwy/data_file/s3_requests/"
    saveto_result_file_path = "/var/luci/foodhwy/data_file/s3_results/"

    fetch_convert_save_request_flow()
