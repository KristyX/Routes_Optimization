import boto3
from pprint import pprint
import json


def get_prefix(timestamp):
    year = timestamp[0:4]
    month = timestamp[4:6]
    day1 = timestamp[6:8]
    if day1[0] == '0':
        day2 = str(0) + str(int(day1[1]) + 1)
    else:
        day2 = int(day1) + 1
    # message/2019-08/day22/
    folder_prefix1 = str(year) + "-" + str(month) + "/" + "day" + str(day1)
    folder_prefix2 = str(year) + "-" + str(month) + "/" + "day" + str(day2)
    prefix1 = origin_request_prefix + folder_prefix1
    prefix2 = origin_request_prefix + folder_prefix2
    print(prefix1, prefix2)
    return [prefix1, prefix2]

def get_json_from_globalid(global_id, target_list):
    for target in target_list:
        if target != {}:
            new_data = target.replace('}{', '},{')
            json_data_list = json.loads(f'[{new_data}]')
            for json_data in json_data_list:
                if json_data["global_id"] == global_id:
                    return json_data
    return {}

def fetch_from_s3bucket(timestamp, city_id, bucket=foodhwy_bucket):
    day = timestamp[:-12]
    global_id_list = []
    prefix_list = get_prefix(timestamp)
    for prefix in prefix_list:
        response = s3client.list_objects(Bucket=bucket, Prefix=prefix)
        s3 = boto3.resource('s3')
        if "Contents" in response:
            for file in response['Contents']:
                fname = file["Key"]
                obj = s3.Object(bucket, fname)
                target =obj.get()['Body'].read().decode('utf-8')
                if target != {}:
                    new_data = target.replace('}{', '},{')
                    json_data_list = json.loads(f'[{new_data}]')
                    for target_json in json_data_list:
                        globalid_time = str(target_json["global_id"]).replace("-", "")
                        if target_json["city_id"] == city_id and target_json["global_id"][:target_json["global_id"].find("-")] == day:
                            print(target_json["global_id"])
                            global_id_list.append(target_json)
    return global_id_list


if __name__ == "__main__":
    # 12 zero
    timestamp = "20190914000000000000"
    city_id = 3612
    fetch_from_s3bucket(timestamp, city_id)
