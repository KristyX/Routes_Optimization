import os
from os.path import isfile,join
import json
def format_json(input_json):
    message = {}
    message['driverinfo'] = {"data":input_json["dirverinfo"]}
    for order in input_json["orderinfo"]:
        message['orderinfo'] = {"data":[order]}
        message['city_id'] = order['city_id']
    return json.dumps(message)

def read_jsons(path, startDate, endDate=None):
    data_jsons = []
    count = 0
    s = str(startDate)
    e = str(endDate)
    for f_name in os.listdir(path):
        count +=1
        if isfile(join(path, f_name)) and not f_name.startswith(".") and f_name[:len(s)]>=s:
            if endDate == None or f_name[:len(e)]<=e:
                with open(path + f_name, "rb") as f:
                    input_json = format_json(json.loads(f.read()))
                    if input_json not in data_jsons:
                        data_jsons.append(input_json)
    return data_jsons

if __name__ == "__main__":
    a = read_jsons("./data_info/",20190809,20190810)
    print(len(a))