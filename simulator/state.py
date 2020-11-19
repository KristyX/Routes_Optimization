import util
import json
import copy
from pprint import pprint

class State:
    def __init__(self, sand_table, batches):
        new_orders = []
        for batch in batches:
            new_orders+=batch
        available_drivers = sand_table.find_update_available_drivers(new_orders[0]["req_time"])
        self.drivers = copy.deepcopy(available_drivers)
        self.new_orders = copy.deepcopy(new_orders)
        self.city_id = new_orders[0]['city_id']
        self.global_id = new_orders[0]["global_id"]
        self.plan = None
        self.aggregate_routes = None
        self.driver_order_table = None

    def apply_action(self,sand_table, action, mod_path):
        req = {"global_id": self.global_id,
               "city_id": self.city_id,
               "driverinfo": {"status": 1, "error": "", "data": self.drivers},
               "orderinfo": {"status": 1, "error": "", "data": self.new_orders},
               "optpara": {"model_path": mod_path},
               "password": '34e5d84836006f8f350dac36221efd2d',
               "reset": True}
        res = action({"body": json.dumps(req)}, None)
        res = json.loads(res["body"])
        plan = [util.drop_by_time_recalculator(opt_plan, sand_table.orders) for opt_plan in res["results"][0]["opt_plan"]]
        self.plan = plan
        sand_table.update_driver_order_table(self)
        # assign new assignments to drivers and update their path
        sand_table.update_routes(self.plan)
        # make a copy of current routes
        self.aggregate_routes = copy.deepcopy(sand_table.routes)

    def calculate_score(self):
        return

    def customers_expected_waiting_time(self,orders):
        routes = self.aggregate_routes
        res = []
        for aid in routes:
            driver_plans = routes[aid]
            for plan in driver_plans:
                res += util.customers_expected_waiting_time(plan["route_plan"], orders)
        return res

    def drivers_efficiency(self, orders):
        res = []
        for key, value in self.aggregate_routes.items():
            d = 0
            e = 0
            i = {}
            for route in value:
                driving_time, estimate_time = util.driving_time_estimate_time(route, orders)
                d += driving_time
                e +=estimate_time
            i["driver_id"] = key
            i["driver_efficiency"] = e/d
            res.append(i)
        return res

    def drivers_ordernum_per_hour(self,environment):
        res = []
        lookup={}
        for driver in environment["driverinfo"]:
            if driver['aid'] not in res:
                lookup[driver["aid"]] = 0
            lookup[driver["aid"]] += driver["end_time"] -driver["start_time"]
        for key, value in self.aggregate_routes.items():
            ordr_num = 0
            i = {}
            for route in value:
                ordr_num+=len(route["route_plan"])/2
            i["driver_id"] = key
            i["order_num_per_hour"] = ordr_num / lookup[key] * 3600
            res.append(i)
        return res

    def total_profit(self,environment):
        return

if __name__ == "__main__":
    req = {'city_id': 3149,
 'driverinfo': {'data': [{'aid': 5913,
                          'avatar': '5913_ccac828804b74eb386294442ac77c31d.jpg',
                          'city_id': 3149,
                          'current_time': 1569267819,
                          'driver_lv': 5,
                          'end_time': 1569267819,
                          'lat': '43.777511303030295',
                          'lng': '-79.41097457575758',
                          'nickname': 'A-Sam Liu SCB',
                          'online': 1,
                          'orders': [],
                          'start_time': 1569252653,
                          'working_day': '2019-09-23'},
                         {'aid': 8427,
                          'avatar': '8427_08398c05ac724623264cd9b65336e41c.jpg',
                          'city_id': 3149,
                          'current_time': 1569267819,
                          'driver_lv': 1,
                          'end_time': 1569282609,
                          'lat': '43.777511303030295',
                          'lng': '-79.41097457575758',
                          'nickname': 'YIHANG QIN',
                          'online': 1,
                          'orders': [],
                          'start_time': 1569252653,
                          'working_day': '2019-09-23'},
                         {'aid': 5444,
                          'avatar': '5444_d0b070177ad4dbdc0a21e1e748bc711b.jpg',
                          'city_id': 3149,
                          'current_time': 1569267819,
                          'driver_lv': 1,
                          'end_time': 1569267819,
                          'lat': '43.777511303030295',
                          'lng': '-79.41097457575758',
                          'nickname': 'Richard Wang WTL',
                          'online': 1,
                          'orders': [],
                          'start_time': 1569252653,
                          'working_day': '2019-09-23'},
                         {'aid': 5450,
                          'avatar': '5450_7b621b00155edbb3a314cf6956c6f765.jpg',
                          'city_id': 3149,
                          'current_time': 1569267819,
                          'driver_lv': 1,
                          'end_time': 1569282609,
                          'lat': '43.7726441',
                          'lng': '-79.4134515',
                          'nickname': 'C-Terry Chen WTL',
                          'online': 1,
                          'orders': [{'city_id': 3149,
                                      'cookingtime_result': 1569266210,
                                      'cookingtime_set': 900,
                                      'created_at': 1569265117,
                                      'customer_id': 796134,
                                      'customer_lat': '43.7726441',
                                      'customer_lng': '-79.4134515',
                                      'distance': 1000,
                                      'global_id': '20190923-150151192989',
                                      'id': 2469227,
                                      'req_time': 1569265311,
                                      'shipping_amount': '0.99',
                                      'shop_addr': '5461 Yonge St, North York, '
                                                   'ON M2N 5S1',
                                      'shop_id': 17045,
                                      'shop_lat': '43.776853',
                                      'shop_lng': '-79.416172',
                                      'shop_name': 'GyuGyuYa',
                                      'shop_tel': '6477485461',
                                      'status': 12,
                                      'subtotal': '16.88'}],
                          'start_time': 1569254477,
                          'working_day': '2019-09-23'}],
                'error': '',
                'status': 1},
 'global_id': '20190923-154339302598',
 'optpara': {'model_path': '../lambda_env/main/foodhwy/foodhwy_sqrt.mod'},
 'orderinfo': {'data': [{'city_id': 3149,
                         'cookingtime_result': 1569268716,
                         'cookingtime_set': 15,
                         'created_at': 1569267650,
                         'customer_id': 815944,
                         'customer_lat': '43.7640292',
                         'customer_lng': '-79.4070417',
                         'distance': 2000,
                         'global_id': '20190923-154339302598',
                         'id': 2469305,
                         'req_time': 1569267819,
                         'shipping_amount': '0.99',
                         'shop_addr': '5443 Yonge Street,',
                         'shop_id': 16260,
                         'shop_lat': '43.777081',
                         'shop_lng': '-79.416789',
                         'shop_name': '越来香',
                         'shop_tel': '4169012586',
                         'status': 10,
                         'subtotal': '10.95'},
                        {'city_id': 3149,
                         'cookingtime_result': 1569270367,
                         'cookingtime_set': 15,
                         'created_at': 1569269437,
                         'customer_id': 805188,
                         'customer_lat': '43.778926',
                         'customer_lng': '-79.4171121',
                         'distance': 4000,
                         'global_id': '20190923-161116107725',
                         'id': 2469352,
                         'req_time': 1569269476,
                         'shipping_amount': '2.49',
                         'shop_addr': '7181 Yonge St Thornhill, ON L3T 0C7',
                         'shop_id': 16592,
                         'shop_lat': '43.802669',
                         'shop_lng': '-79.422305',
                         'shop_name': '贼好吃的烤肉饭',
                         'shop_tel': '9055976768',
                         'status': 10,
                         'subtotal': '39.93'}],
               'error': '',
               'status': 1},
 'password': '34e5d84836006f8f350dac36221efd2d',
 'reset': True}

    pprint(req)

    import sys

    sys.path.append('../lambda_env/main/')
    import region3141 as region

    res = region.lambda_handler({"body": json.dumps(req)}, None)
    print(res)
