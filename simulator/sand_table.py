import numpy as np
import pandas as pd
import locator_helper
import util
import copy
from sklearn.cluster import AgglomerativeClustering
from pprint import pprint

class SandTable:
    def __init__(self, drivers, orders):
        self.drivers = copy.deepcopy(drivers)
        self.orders = copy.deepcopy(orders)
        shops = []
        order_batches = {}
        o = {}
        for order in self.orders:
            shop = [order["shop_lat"], order["shop_lng"]]
            if shop not in shops:
                shops.append(shop)
            o[order["id"]] = order
            if order["global_id"] in order_batches:
                order_batches[order["global_id"]].append(order)
            else:
                order_batches[order["global_id"]] = [order]
        self.shops = shops
        shops = np.array(shops).astype(float)
        clustering = AgglomerativeClustering(n_clusters=None, compute_full_tree=True,
                                             affinity=lambda x: locator_helper._calculate_driving_time(x,x), linkage='complete',
                                             distance_threshold=60)
        clustering.fit(shops)
        labels = np.unique(clustering.labels_)
        plaza = []
        for l in labels:
            #plaza.append(np.mean(shops[clustering.labels_ == l], axis=0))

            if len(shops[clustering.labels_ == l]) > 1:
                plaza.append(np.mean(shops[clustering.labels_ == l], axis=0))
        plaza = np.array(plaza)
        self.plaza = plaza
        # lookup dirver index table
        order_batches = [order_batches[batch] for batch in order_batches]
        order_batches.sort(key=lambda val: val[0]["global_id"])
        self.batches = order_batches
        self.drivers_table = {}
        self.order_table = o
        self.routes = {}
        self.driver_order_table = pd.DataFrame()

    def find_update_available_drivers(self,current_time):
        """Return and Update drivers who is available at current_time"""
        available_drivers = []
        self.drivers_table = {}
        count = 0
        for driver in self.drivers:
            if util.check_available(driver, current_time):
                count+=1
                self.drivers_table[count] = driver
                if "current_time" not in driver:
                    driver["current_time"] = driver["start_time"]
                t = current_time - driver["current_time"]
                driver["current_time"] = current_time
                # update driver location
                if driver["aid"] in self.routes:
                    # driver's latest route
                    route = self.routes[driver["aid"]][-1]
                    # find next point B
                    for cur in route["route_plan"]:
                        # check if driver already passed cur location
                        if cur["drop_by_end_time"] < current_time:
                            util.update_driver_order(driver, cur)
                            driver["lat"], driver["lng"] = cur["drop_by"]
                        else:
                            # time passed
                            driver["lat"], driver["lng"] = util.calculate_new_location(driver, cur, t)
                            break
                # if driver has no order, then they can drive to anywhere they want. For now, they drive to the closest plaza.
                if not driver["orders"]:
                    driver["lat"], driver["lng"] = util.random_walk(driver, self.plaza, t)
                available_drivers.append(driver)
        return available_drivers

    def update_routes(self, assignments):
        """Update new assignments to routes"""
        aggregate_routes = self.routes
        drivers_table = self.drivers_table
        for assignment in assignments:
            # add new order to the driver
            # find driver
            idx = assignment["idx"]
            driver_id = assignment["driver_id"]
            driver = drivers_table[idx]
            # get new order and update info
            order_id = assignment["assigned_order_id"]
            order = self.order_table[order_id]
            order["cookingtime_set"] *= 60 #because cookingtime_set is mins in a new order, but seconds in drivers' order
            order["status"] = 15
            driver["orders"].append(order)
            set_b = set()
            for b in assignment['route_plan']:
                if b["drop_by_type"] == "NS":
                    b["drop_by_type"] = "S"
                elif b["drop_by_type"] == "NC":
                    b["drop_by_type"] = "C"
                set_b.add((b["order_id"], b["drop_by_type"]))
            # add new driver to aggregate_routes
            if driver_id not in aggregate_routes:
                aggregate_routes[driver_id] = []
            all_routes = aggregate_routes[driver_id]
            if all_routes != []:
                route = all_routes[-1]
                passed = []
                overlap = 0
                for i in route['route_plan']:
                    if (i["order_id"], i["drop_by_type"]) not in set_b:
                        passed.append(i)
                    else:
                        overlap += 1
                # If there is a overlap in aggregate_routes, then driver is still in on this route. We add new routes after passed places.
                if overlap > 0:
                    route['route_plan'] = passed + assignment['route_plan']
                    route["assigned_order_id"] = assignment["assigned_order_id"]
                # If there is no overlap, then this is a new route.
                else:
                    all_routes.append(assignment)
            else:
                all_routes.append(assignment)

    def update_driver_order_table(self, state):
        driver_assign_table = []
        new_order_table = pd.DataFrame(state.new_orders)
        for p in state.plan:
            #order = new_order_table[new_order_table["id"] == p["assigned_order_id"]]
            #print("req_time",order["req_time"].values.tolist()[0],"order_id",p["assigned_order_id"], "shop",order[["shop_lat","shop_lng"]].values.astype(float).tolist(),"driver_id",p["driver_id"], "driver_location",p["driver_location"][0],p["driver_location"][1])
            driver_assign_table.append({"id": p["assigned_order_id"], "driver_lat": p["driver_location"][0],
                                        "driver_lng": p["driver_location"][1]})
        driver_assign_table = pd.DataFrame(driver_assign_table)
        new_order_table["cookingtime_set"] = new_order_table["cookingtime_set"]*60
        driver_order_table = pd.merge(driver_assign_table, new_order_table, on=["id"])
        state.driver_order_table = pd.concat([self.driver_order_table, driver_order_table], axis=0)
        self.driver_order_table = state.driver_order_table
