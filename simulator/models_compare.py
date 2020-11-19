import virtual_reality as vr
import sys
import pandas as pd
sys.path.append('../lambda_env/main/')
import region3141 as region
import evaluation
import os
import real_routes_analysis as rra
import json
from pprint import pprint

current_path = os.getcwd()
MODELS_DIR = current_path + "/../lambda_env/main/foodhwy/"
def format_summary_table(aggregate_routes, driver_order_table, model_name, config):
    order_summary_columns = config["order_summary_columns"]
    route_summary_columns = config["route_summary_columns"]
    aggregate_route_columns = config["aggregate_route_columns"]
    order_summary_table, route_summary_table, aggregate_route_summary_table = evaluation.aggregate_route_summary(
        aggregate_routes, driver_order_table)
    if order_summary_columns == "*":
        order_summary_columns = list(order_summary_table.keys())
    if route_summary_columns == "*":
        route_summary_columns = list(route_summary_table.keys())
    if aggregate_route_columns == "*":
        aggregate_route_columns = list(aggregate_route_summary_table.keys())
    if "order_id" not in order_summary_columns:
        order_summary_columns = ["order_id"] + order_summary_columns
    if "driver_id" not in route_summary_columns:
        route_summary_columns = ["driver_id"] + route_summary_columns
    if "driver_id" not in aggregate_route_columns:
        aggregate_route_columns = ["driver_id"] + aggregate_route_columns
    order_summary_table = order_summary_table[order_summary_columns]
    route_summary_table = route_summary_table[route_summary_columns]
    aggregate_route_summary_table = aggregate_route_summary_table[aggregate_route_columns]

    order_summary_columns.remove("order_id")
    order_summary_columns = ["order_id"] + list(map(lambda x: x + "_" + model_name, order_summary_columns))
    order_summary_table.columns = order_summary_columns
    route_summary_columns.remove("driver_id")
    route_summary_columns = ["driver_id"] + list(map(lambda x: x + "_" + model_name, route_summary_columns))
    route_summary_table.columns = route_summary_columns
    aggregate_route_columns.remove("driver_id")
    aggregate_route_columns = ["driver_id"] + list(map(lambda x: x + "_" + model_name, aggregate_route_columns))
    aggregate_route_summary_table.columns = aggregate_route_columns
    return order_summary_table, route_summary_table, aggregate_route_summary_table

def _models_compare_helper(date, city_id, model, config):
    num_batch = config["num_batch"]
    environment = vr.create_environment(date, city_id)
    model_name = model[:model.rfind(".")]
    model_path = MODELS_DIR + model
    states = vr.main(environment, region.lambda_handler, model_path, num_batch=num_batch)
    state = states[-1]
    return format_summary_table(state.aggregate_routes, state.driver_order_table, model_name, config)

def models_compare(date, city_id, model_names, config):
    aggregate_routes, driver_order_table = rra.real_aggregate_routes(date, city_id)
    order_summary, route_summary, aggregate_route_summary = format_summary_table(aggregate_routes, driver_order_table, "real", config)

    for model in model_names:
        order_summary_table, route_summary_table, aggregate_route_summary_table = _models_compare_helper(date, city_id,
                                                                                                         model,
                                                                                                         config)
        order_summary = pd.merge(order_summary, order_summary_table, how="outer", on="order_id")
        route_summary = pd.merge(route_summary, route_summary_table, how="outer", on="driver_id")
        aggregate_route_summary =  pd.merge(aggregate_route_summary, aggregate_route_summary_table, how="outer", on="driver_id")

    folder_path = "./simulator_csv/"
    order_summary_file_name = str(city_id) + "-" + str(date) + "-order_summary"+".csv"
    route_summary_file_name = str(city_id) + "-" + str(date) + "-route_summary"+".csv"
    aggregate_route_summary_file_name = str(city_id) + "-" + str(date) + "-aggregate_route_summary"+".csv"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    with open(folder_path+order_summary_file_name, "w") as f:
        f.write(order_summary.to_csv(index=False))
    with open(folder_path+route_summary_file_name, "w") as f:
        f.write(route_summary.to_csv(index=False))
    with open(folder_path+aggregate_route_summary_file_name, "w") as f:
        f.write(aggregate_route_summary.to_csv(index=False))
    print(order_summary)
    print(route_summary)
    print(aggregate_route_summary)
    print(order_summary.describe())
    print(route_summary.describe())
    print(aggregate_route_summary.describe())

def aggregate_routes_compare(aggregate_routes, simulator_aggregate_routes, driver_orders_table):
    real_drop_by = []
    fake_drop_by = []
    for driver_id in aggregate_routes:
        for real_route, fake_route in zip(aggregate_routes[driver_id], simulator_aggregate_routes[driver_id]):
            real_drop_by += real_route["route_plan"]
            fake_drop_by += fake_route["route_plan"]
    real_table = pd.DataFrame(real_drop_by)
    real_table["created_at"] = pd.merge(real_table, driver_orders_table,  left_on=["order_id"], right_on=["id"])[
        "created_at"]
    real_table["ETA"] = real_table["drop_by_end_time"] - real_table["created_at"]
    real_table["waiting_time"] = real_table["drop_by_end_time"] - real_table["drop_by_start_time"]
    fake_table = pd.DataFrame(fake_drop_by)
    fake_table["created_at"] = pd.merge(fake_table, driver_orders_table, left_on=["order_id"], right_on=["id"])[
        "created_at"]
    fake_table["ETA"] = fake_table["drop_by_end_time"] - fake_table["created_at"]
    fake_table["waiting_time"] = fake_table["drop_by_end_time"] - fake_table["drop_by_start_time"]
    res = pd.merge(real_table, fake_table, on=["order_id","drop_by_id"], suffixes=["_real", "_fake"])
    res["end_time_difference"] = res["drop_by_end_time_fake"] - res["drop_by_end_time_real"]
    res["ETA_difference"] = res["ETA_fake"] - res["ETA_real"]
    from scipy import stats
    print(stats.ks_2samp(check["ETA_real"], check["ETA_fake"]))
    return res
if __name__ == "__main__":
    date = "20190926"
    # city_id = 3613 #dt
    city_id = 3149 #yn
    #city_id = 3612 #windsor
    #list all models to compare
    model_names = ["foodhwy_pwlb3.mod","foodhwy_sowt.mod","foodhwy_sqrt.mod",]
    #select want to see
    config = {"num_batch":1,
              "order_summary_columns": ["relative_delivery_time"],
              "route_summary_columns": ["total_driving_time"],
              "aggregate_route_columns": ["driving_efficiency_on_time"]}
    models_compare(date,city_id,model_names, config)

    # with open("./aggregate_routes/real.json", "r") as f:
    #     aggregate_routes = json.loads(f.read())
    # with open("./aggregate_routes/fake.json", "r") as f:
    #     simulator_aggregate_routes = json.loads(f.read())
    # driver_orders_table = pd.read_csv("./driver_orders/"+str(city_id)+"-"+date)
    # res = aggregate_routes_compare(aggregate_routes, simulator_aggregate_routes, driver_orders_table)
    # check = res[["order_id","drop_by_id","created_at_real","drop_by_type_real", "drop_by_end_time_real","drop_by_end_time_fake","ETA_real","ETA_fake","ETA_difference", "end_time_difference"]]
    # check.drop_by_end_time_real = check.drop_by_end_time_real.astype(int)
    # check.drop_by_end_time_fake = check.drop_by_end_time_fake.astype(int)
    # from scipy import stats
    # print(stats.ks_2samp(check["ETA_real"], check["ETA_fake"]))
