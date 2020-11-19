from config import *
import pandas as pd
import json
from pprint import pprint
import requests


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


def get_latest_global_id():
    # sql = "SELECT global_id FROM foodhwy_pro.mvp_driver_dashboard ORDER BY global_id DESC"
    # fhw_cursor.execute(sql)
    # latest_global_id = fhw_cursor.fetchone()[0]
    # print(latest_global_id)
    latest_global_id = "20190816-000004822881"
    return latest_global_id


def get_info_from_opt_result(opt_result):
    global_id = opt_result["global_id"]
    # Only calculate the best solution if more than 1 solution found
    opt_plan = opt_result["results"][0]["opt_plan"][0]
    assigned_driver = opt_plan["driver_id"]
    status_code = opt_result["status"]
    num_sol = opt_result["message"][0]
    dic = {"global_id": [global_id], "best_driver": [assigned_driver], "status_code": [status_code],
           "num_sol": [num_sol]}
    df = pd.DataFrame.from_dict(dic)
    return df


def get_info_from_err_log(err_log):
    global_id = err_log["global_id"]
    status_code = err_log["status"]
    best_driver = -1
    num_sol = 0
    df = pd.DataFrame.from_dict(
        {"global_id": [global_id], "best_driver": [best_driver], "status_code": [status_code], "num_sol": [num_sol]})
    return df


def fetch_result_from_s3(latest_global_id, bucket, prefix):
    latest_timestamp = latest_global_id.replace("-", "")
    response = s3client.list_objects(Bucket=bucket, Prefix=prefix)
    target_result_list = []
    if "Contents" in response:
        for file in response['Contents']:
            try:
                fname = file["Key"]
                obj = s3.Object(bucket, fname)
                target = obj.get()['Body'].read().decode('utf-8')
                if target != {}:
                    new_data = target.replace('}{', '},{')
                    result_list = json.loads(f'[{new_data}]')
                    for target_result in result_list:
                        globalid_time = str(target_result["global_id"]).replace("-", "")
                        if globalid_time > latest_timestamp:
                            target_result_list.append(target_result)
            except:
                pass
    return target_result_list


def fetch_convert_save_result_flow(target_prefix):
    # __________________________________________________________________________________________
    # get all the directories need to be scan, and where to start
    latest_global_id = get_latest_global_id()
    prefix_list = get_prefix_list(latest_global_id, foodhwy_bucket, target_prefix)
    for month_prefix in prefix_list:
        date_prefix_list = get_prefix_list(latest_global_id, foodhwy_bucket, month_prefix)
        for date_prefix in date_prefix_list:
            df = pd.DataFrame([])
            print(date_prefix)
            opt_result_list = fetch_result_from_s3(latest_global_id, foodhwy_bucket, date_prefix)
            for opt_result in opt_result_list:
                if target_prefix == result_prefix:
                    # for opt result use
                    df_tmp = get_info_from_opt_result(opt_result)
                elif target_prefix == error_prefix:
                    # for error log use
                    df_tmp = get_info_from_err_log(opt_result)
                df = df.append(df_tmp)
            if len(df) != 0:
                print(df)
                df.to_sql("opt_solution_from_s3", con=fhw_engine, if_exists="append", index=False)


if __name__ == "__main__":
    saveto_result_file_path = "/var/luci/foodhwy/data_file/s3_results/"
    fetch_convert_save_result_flow(result_prefix)
    fetch_convert_save_result_flow(error_prefix)
