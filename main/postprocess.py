import json
import pandas as pd
from pprint import pprint

def output_helper(opt_output):
    sol = opt_output.split("\n")
    new_assignments = []
    routes = None
    for i in sol[:-1]:
        item = eval(i)
        if isinstance(item, list):
            new_assignments.append(item)
        else:
            routes = item
    return new_assignments, routes


def format_output(data, opt_output):
    res = []
    driver_table = data["dataframes"]["driver_table"]
    driver_order_table = data["dataframes"]["driver_order_table"]
    new_order_table = data["dataframes"]['new_order_table']
    new_assignment, routes = output_helper(opt_output)
    for driver in routes:
        driver_route = {"driver_id": driver}
        aid_mask = driver_table["aid"] == driver
        driver_route["driver_location"] = driver_table[["lat", "lng"]][aid_mask].values.tolist()[0]
        driver_route["nickname"] = driver_table[["nickname"]][aid_mask].values.tolist()[0][0]
        opt_plan = []
        for r in routes[driver]:
            location = {"order_id": r[0], "drop_by_type": r[1], "drop_by_estimate_time": r[2]}
            if r[1] == "NS":
                order = new_order_table[new_order_table["id"] == r[0]]
                loc, drop_by_id = ["shop_lat", "shop_lng"], "shop_id"
            elif r[1] == "NC":
                order = new_order_table[new_order_table["id"] == r[0]]
                loc, drop_by_id = ["customer_lat", "customer_lng"], "customer_id"
            elif r[1] == "S":
                order = driver_order_table[driver_order_table["id"] == r[0]]
                loc, drop_by_id = ["shop_lat", "shop_lng"], "shop_id"
            elif r[1] == "C":
                order = driver_order_table[driver_order_table["id"] == r[0]]
                loc, drop_by_id = ["customer_lat", "customer_lng"], "customer_id"
            location["drop_by"] = order[loc].values.tolist()[0]
            location["drop_by_id"] = order[drop_by_id].values.tolist()[0]
            opt_plan.append(location)
        driver_route["opt_plan"] = opt_plan
        res.append(driver_route)
    return res, new_assignment


def format_lambda_output(data, opt_output):
    current_time = data["current_time"]
    result = {}
    result["opt_plan"] = []
    assignments, routes = output_helper(opt_output)
    driver_table = data["dataframes"]['driver_table']
    driver_order_table = data["dataframes"]['driver_order_table']
    new_order_table = data["dataframes"]['new_order_table']
    for assignment in assignments:
        route_plan = []
        for r in routes[assignment[0]]:
            location = {}
            if r[1] == "NS":
                order = new_order_table[new_order_table["id"] == r[0]]
                loc, drop_by_id = ["shop_lat", "shop_lng"], "shop_id"
            elif r[1] == "NC":
                order = new_order_table[new_order_table["id"] == r[0]]
                loc, drop_by_id = ["customer_lat", "customer_lng"], "customer_id"
            elif r[1] == "S":
                order = driver_order_table[driver_order_table["id"] == r[0]]
                loc, drop_by_id = ["shop_lat", "shop_lng"], "shop_id"
            elif r[1] == "C":
                order = driver_order_table[driver_order_table["id"] == r[0]]
                loc, drop_by_id = ["customer_lat", "customer_lng"], "customer_id"
            location["drop_by_type"] = r[1]
            location["order_id"] = r[0]
            location["drop_by"] = order[loc].values.tolist()[0]
            location["drop_by_id"] = order[drop_by_id].values.tolist()[0]
            route_plan.append(location)
        driver = driver_table[driver_table["idx"] == assignment[0]]
        result["opt_plan"].append({"driver_id": driver["aid"].values.tolist()[0],
                                   "idx": assignment[0],
                               "assigned_order_id": assignment[1],
                               "driver_location":driver[["lat", "lng"]].values.tolist()[0],
                               "driver_start_time":current_time,
                               "route_plan": route_plan})
        result["reject_conditon"] = [{"driver_id":  driver_table[driver_table["idx"] == row[1]][["aid"]].values.tolist()[0][0], "order_id": row[0]} for row in data["dataframes"]['reject_table'][["order_id", "idx"]].values.tolist()]
    df_assign = pd.DataFrame(assignments, columns=["idx", "order_id", "driver_id"])
    return result, df_assign


if __name__ == "__main__":
    with open("./input_jsons/input_json_20190822-115135073724.json") as f:
        req = json.loads(f.read())
    # logistics_params = region3141.preprocess(req)
    # data = region3141.produce_data(logistics_params)
    # opt_output = opt_algorithm.opt_alg("../cplex/oplrun", "./foodhwy.mod", "./failed_dats/20190822-115135073724.dat")["opl"]
    # pprint(opt_output)
    # result,df_assign = format_lambda_output(data, opt_output)
    # pprint(result)
    # print(df_assign)
    '''
    with open("./output_jsons/test.json","w+") as f:
        f.write(json.dumps(result))
    '''
