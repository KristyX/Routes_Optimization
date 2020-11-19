import boto3
import json
from pprint import pprint

month_31 = [1, 3, 5, 7, 8, 10, 12]
month_30 = [4, 6, 9, 11]


def get_json_from_globalid(global_id, target_list):
    for target in target_list:
        if target != {}:
            new_data = target.replace('}{', '},{')
            json_data_list = json.loads(f'[{new_data}]')
            for json_data in json_data_list:
                if json_data["global_id"] == global_id:
                    return json_data
    return {}


def parse_globalid(global_id):
    year = int(global_id[:4])
    month = int(global_id[4:6])
    day = int(global_id[6:8])
    hour = int(global_id[9:11])
    min = int(global_id[11:13])
    second = int(global_id[13:])
    hour = hour + 4
    if hour >= 24:
        hour = hour - 24
        day = day + 1

    if month in month_31:
        if day > 31:
            day = day - 31
            month = month + 1
    elif month in month_30:
        if day > 30:
            day = day - 30
            month = month + 1
    else:
        if ((year % 4 == 0 and year % 100 != 0) or (year % 400 == 0 and year % 3200 != 0)):
            if day > 29:
                day = day - 29
                month = month + 1
        else:
            if day > 28:
                day = day - 28
                month = month + 1
    if month > 12:
        month = month - 12
        year = year + 1

    return get_str(year), get_str(month), get_str(day), get_str(hour), get_str(min), get_str(second)


def get_str(num):
    if num < 10:
        str_num = "0" + str(num)
        return str_num
    else:
        return str(num)


def fetch_from_s3bucket(global_id, firehose_prefix_info, bucket=foodhwy_bucket):
    prefix, target_file1, target_file2, target_file3 = get_file_name_prefix(global_id, firehose_prefix_info)
    target1 = {}
    target2 = {}
    target3 = {}
    response = s3client.list_objects(Bucket=bucket, Prefix=prefix)
    s3 = boto3.resource('s3')
    if "Contents" in response:
        for file in response['Contents']:
            fname = file["Key"]
            if str(target_file1) in str(fname):
                obj = s3.Object(bucket, fname)
                target1 = obj.get()['Body'].read().decode('utf-8')
            if str(target_file2) in str(fname):
                obj = s3.Object(bucket, fname)
                target2 = obj.get()['Body'].read().decode('utf-8')
            if str(target_file3) in str(fname):
                obj = s3.Object(bucket, fname)
                target3 = obj.get()['Body'].read().decode('utf-8')
    return target1, target2, target3


def get_file_name_prefix(global_id, firehose_prefix_info):  # firehose_prefix_info is a list of 2 elements
    year, month, day, hour, min, second = parse_globalid(global_id)
    prefix = []
    file_name_prefix2 = ""
    file_name_prefix3 = ""
    prefix1 = firehose_prefix_info[0] + year + "-" + month + "/" + "day" + day + "/"
    prefix.append(prefix1)
    file_name_prefix1 = firehose_prefix_info[1] + "-" + year + "-" + month + "-" + day + "-" + hour + "-" + min
    # TODO: may conflict if cross a day

    try:
        min = get_str(int(int(min) - 1))
        file_name_prefix2 = firehose_prefix_info[1] + "-" + year + "-" + month + "-" + day + "-" + hour + "-" + min
    except Exception:
        pass

    try:
        min = get_str(int(int(min) + 2))
        file_name_prefix3 = firehose_prefix_info[1] + "-" + year + "-" + month + "-" + day + "-" + hour + "-" + min
    except Exception:
        pass
    return prefix[0], file_name_prefix1, file_name_prefix2, file_name_prefix3


def track_flow(global_id):
    msg_target1, msg_target2, msg_target3 = fetch_from_s3bucket(global_id, firehose_message)
    input_data = get_json_from_globalid(global_id, [msg_target1, msg_target2, msg_target3])
    sync_target1, sync_target2, sync_target3 = fetch_from_s3bucket(global_id, firehose_persist)
    sync_data = get_json_from_globalid(global_id, [sync_target1, sync_target2, sync_target3])
    res_target1, res_target2, res_target3 = fetch_from_s3bucket(global_id, firehose_opt_log)
    result_data = get_json_from_globalid(global_id, [res_target1, res_target2, res_target3])
    restore_json = {"input_data": input_data, "sync": sync_data, "result": result_data}
    # pprint(restore_json)

    # with open("/var/luci/foodhwy/data_file/json/" + global_id + ".json", 'w') as fp:
    #     json.dump(restore_json, fp)
    return restore_json

if __name__ == "__main__":
    global_id = "20190822-122459657445"
    track_flow(global_id)
