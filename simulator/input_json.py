import pandas as pd
import os
import datetime
import numpy as np
import json
from pprint import pprint

def convert_unixtime_to_datetime(timestamp):
    start = datetime.datetime.fromtimestamp(timestamp)
    end = start + datetime.timedelta(days=1)
    return start.strftime("%Y%m%d%H%M%S%f")+".json", end.strftime("%Y%m%d%H%M%S%f")+".json"

def model_input(created_at, order_id, onlyfiles):
    start, end = convert_unixtime_to_datetime(created_at)
    consider = onlyfiles[onlyfiles>=start]
    consider = consider[consider<end]
    consider = np.sort(consider)
    for c in consider:
        with open("./data_info/"+c) as f:
            j = json.loads(f.read())
            for driver in j["dirverinfo"]:
                for order in driver["orders"]:
                    if order["id"] == order_id:
                        driver["orders"].remove(order)
                        order["status"] = 10
                        order["cookingtime_set"] = order["cookingtime_set"]//60
                        return {"city_id":order["city_id"],"driverinfo":{"status":1,"error":"","data":j["dirverinfo"]},"orderinfo": {"status": 1, "error": "", "data":[order]}}


if __name__ == "__main__":
    luci_orders_view = pd.read_csv("luci_orders_view.csv")
    luci_orders_view = luci_orders_view[luci_orders_view["delivered_at"].notnull()]
    luci_orders_view = luci_orders_view[luci_orders_view.created_at > 1565222400]
    luci_orders_view = luci_orders_view[luci_orders_view["city_id"] == 3621]
    onlyfiles = np.array([f for f in os.listdir("./data_info") if os.path.isfile(os.path.join("./data_info", f)) and not f.startswith(".")])
    for created_at, order_id in luci_orders_view[["created_at","order_id"]].values:
        res = model_input(created_at, order_id,onlyfiles)
        pprint(res)
        if res != None:
            with open("./timestamp_json1/"+convert_unixtime_to_datetime(created_at)[0],"w") as f:
                f.write(json.dumps(res))
