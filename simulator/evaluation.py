import locator_helper
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def estimate_late_time(distance):
    if distance <= 3000:
        return 35
    elif distance <= 5000:
        return 45
    elif distance <= 10000:
        return 70
    else:
        return 85

def route_summary(route, inputs):
    """
    Summarize each order info from the given route and corresponding inputs.
    Outputs are order_summary_table and route_summary_table, and they are in DataFrame.
    The order summary table includes following information:
    order_id: id of a order
    shop_id: id of the shop corresponding to the order_id
    customer_id: id of the customer corresponding to the order_id
    driver_id: id of a driver who gets the order
    pickup_driving_time: the time from the driver who accepts the order to arrive at the shop
    pickup_driving_distance: the distance from the driver who accepts the order to arrive at the shop
    waiting_time_at_shop: the time that the driver waits at the shop
    pickup_to_delivery_time: the time from the driver leaves the shop to arrive at the customer
    pickup_to_delivery_distance: the distance from the driver leaves the shop to arrive at the customer
    waiting_time_at_customer: the time that the driver waits at the customer
    shortest_time: the shortest driving time from the shop to the customer
    shortest_distance: the shortest driving distance from the shop to the customer
    delivery_time: the time from the customer created the order to they get the order
    cooking_time: order's cooking time
    income: driver's income of this order
    estimate_distance: distance between the shop and the customer in the input
    expected_delivery_time: the time that expected to be delivered

    The route summary table includes following information:
    driver_id: id of a driver who drives on this route
    total_time: the total time that the driver spend on this route
    total_distance: the total that the driver drive on this route
    total_waiting_time: the time that the driver wait on this route
    total_driving_time: the time that the driver drives on this route
    Note:
        1. The unit of time is in second, and the unit of distance is in meter.
        2. If the data cannot be generate, then set it to NaN
    """
    order_summary_table = []
    route_summary_table = {}
    route_summary_table["driver_id"] = route["driver_id"]
    route_summary_table["total_time"] = route["route_plan"][-1]["drop_by_end_time"] - route["driver_start_time"]
    total_distance = 0
    total_waiting_time = 0
    total_driving_time = 0
    prev_time = route["driver_start_time"]
    memory = {}
    for drop in route["route_plan"]:
        total_driving_time += drop["drop_by_start_time"] - prev_time
        total_distance += drop["distance"]
        total_waiting_time += drop["drop_by_end_time"] - drop["drop_by_start_time"]
        for key, value in memory.items():
            value["pickup_to_delivery_time"] += drop["drop_by_end_time"] - prev_time
            value["pickup_to_delivery_distance"] += drop["distance"]
        driver_order = inputs[drop["order_id"] == inputs["id"]]
        if drop["drop_by_type"] == 'S' or drop["drop_by_type"] == 'NS':
            driver_location = driver_order[["driver_lat", "driver_lng"]].values.tolist()[0]
            shop_location = driver_order[["shop_lat", "shop_lng"]].values.tolist()[0]
            pickup_time, pickup_distance = locator_helper.get_trip_time_distance(driver_location, shop_location)
            customer_location = driver_order[["customer_lat", "customer_lng"]].values.tolist()[0]
            shortest_time, shortest_distance = locator_helper.get_trip_time_distance(shop_location, customer_location)
            row = {"order_id": drop["order_id"],
                   "shop_id": drop["drop_by_id"],
                   "driver_id": route["driver_id"],
                   "pickup_driving_time": pickup_time,
                   "pickup_driving_distance": pickup_distance,
                   "waiting_time_at_shop": drop["drop_by_end_time"]-drop["drop_by_start_time"],
                   "pickup_to_delivery_time": 0,
                   "pickup_to_delivery_distance": 0,
                   "shortest_time": shortest_time,
                   "shortest_distance": shortest_distance,
                   "cooking_time": driver_order["cookingtime_set"].values[0],
                   "estimate_distance": driver_order["distance"].values[0],
                   "expected_delivery_time": estimate_late_time(driver_order["distance"].values[0])*60,
                   "income": float(driver_order["shipping_amount"].values[0])
                   }
            memory[drop["order_id"]] = row
        elif drop["drop_by_type"] == 'C' or drop["drop_by_type"] == 'NC':
            row = memory[drop["order_id"]]
            row["waiting_time_at_customer"] = drop["drop_by_end_time"] - drop["drop_by_start_time"]
            row["delivery_time"] = drop["drop_by_end_time"] - driver_order["created_at"].values[0]
            row = memory.pop(drop["order_id"])
            order_summary_table.append(row)
        prev_time = drop["drop_by_end_time"]
    order_summary_table = pd.DataFrame(order_summary_table)
    order_summary_table["relative_delivery_time"] = order_summary_table["delivery_time"] - order_summary_table["cooking_time"] - order_summary_table["shortest_time"]
    route_summary_table["total_distance"] = total_distance
    route_summary_table["total_waiting_time"] = total_waiting_time
    route_summary_table["total_driving_time"] = total_driving_time
    route_summary_table = pd.DataFrame([route_summary_table])
    return order_summary_table, route_summary_table

def aggregate_route_summary(aggregate_route, inputs):
    """
    Summarize each order info from the given route and corresponding inputs.
    Outputs are order_summary_table, route_summary_table, aggregate_route_summary_table, and they are in DataFrame.
    order_summary_table and route_summary_table are as same as defined in function route_summary!

    The aggregate route summary table includes following information:
    driver_id: id of a driver who drives on this route
    total_time: the total time that the driver spend on his aggregate route
    total_distance: the total distance that the driver drive
    total_waiting_time: the total time that the driver wait
    total_driving_time: the total time that the driver drives on this aggregate route
    total_order_num: total number of orders that the driver got on this day
    income_per_hour: total income / total time in hour
    driving_efficiency_on_distance: sum of shortest distance / sum of total driving distance
    driving_efficiency_on_time: sum of shortest driving time / sum of total driving time
    order_num_per_hour: total number of orders / total driving time in hour
    """
    order_summary_table = pd.DataFrame()
    route_summary_table = pd.DataFrame()
    for aid, routes in aggregate_route.items():
        for route in routes:
            order_table, route_table = route_summary(route, inputs)
            order_summary_table = pd.concat([order_summary_table, order_table], axis=0)
            route_summary_table = pd.concat([route_summary_table, route_table], axis=0)
    order_summary_table_group = order_summary_table.groupby(["driver_id"])
    route_summary_table_group = route_summary_table.groupby(["driver_id"])
    total_order_num = order_summary_table_group[["order_id"]].count()
    driver_id = np.array([i for i in route_summary_table_group.groups.keys()]).reshape(-1,1)
    total_in_order = order_summary_table_group[["shortest_time","shortest_distance","income"]].sum()
    total_in_route = route_summary_table_group[["total_time","total_distance","total_waiting_time","total_driving_time"]].sum()
    income_per_hour = total_in_order[["income"]].values / total_in_route[["total_time"]].values * 60
    driving_efficiency = total_in_order[["shortest_distance","shortest_time"]].values / total_in_route[["total_distance","total_driving_time"]].values
    order_num_per_hour = total_order_num.values / total_in_route[["total_time"]].values * 60
    total_income = total_in_order[["income"]].values
    aggregate_route_summary_table = np.concatenate([driver_id, total_in_route.values, total_order_num.values, total_income, income_per_hour,driving_efficiency,order_num_per_hour], axis=1)
    aggregate_route_summary_table = pd.DataFrame(aggregate_route_summary_table, columns=["driver_id",
                                                                                         "total_time",
                                                                                         "total_distance",
                                                                                         "total_waiting_time",
                                                                                         "total_driving_time",
                                                                                         "total_order_num",
                                                                                         "total_income",
                                                                                         "income_per_hour",
                                                                                         "driving_efficiency_on_distance",
                                                                                         "driving_efficiency_on_time",
                                                                                         "order_num_per_hour",])
    return order_summary_table, route_summary_table, aggregate_route_summary_table
