import random
import time
from pprint import pprint
import requests
import json
regions={0:{"lat":(-90,90),"lng":(-180,180)},
         3141:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3142:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3149:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3152:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3606:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3609:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3610:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3611:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3612:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3613:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3614:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3615:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3616:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3617:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3618:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3619:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},
         3666:{"lat":(43.5810245, 43.8554579),"lng":(-79.639219,-79.1168971)},}
def lat_lng_generator(city_id, bounds=regions):
    bound = bounds[city_id]
    lats = bound["lat"]
    lngs = bound["lng"]
    return random.uniform(lats[0], lats[1]), random.uniform(lngs[0], lngs[1])

def generate_order(city_id, order_id):
    within_hour=random.randint(0,7200)
    customer_id = random.randint(100000,999999)
    shop_id=random.randint(0,999999)
    cookingtime_set=random.randint(900,3600)
    #cookingtime_set = 0

    created_at = int(time.time()) + within_hour
    #created_at = 0
    customer_lat,customer_lng = lat_lng_generator(city_id)
    shop_lat, shop_lng = lat_lng_generator(city_id)

    order = {
        "id": order_id, #must be 5 integer
        "status":10,
        "city_id": city_id, #unchange
        "created_at": created_at, #range in 2 hour
        "cookingtime_set": cookingtime_set, # 15 -60 mins
        "cookingtime_result": created_at+cookingtime_set, # if created_at + cookingtime_set
        "customer_id": customer_id, # 6 digits
        "customer_lat": str(customer_lat), #43 - 45
        "customer_lng": str(customer_lng), # -78 - -80
        "shop_id": shop_id, # digits
        "shipping_amount": str(order_id) +" shipping_amount", #
        "subtotal": str(order_id) +" subtotal",
        "shop_lat": str(shop_lat), #same as customer_lat customer_lng
        "shop_lng": str(shop_lng),
        "distance": "distance "+str(order_id),
        "shop_name": "name "+str(order_id),
        "shop_tel": "shop_tel "+str(order_id),
        "shop_addr": "address "+str(order_id)
    }
    return order

def generate_driver(aid, city_id, order_ids):
    driver_lat, driver_lng = lat_lng_generator(city_id)
    driver = {"aid": aid,
              "nickname": "测试配送员"+str(aid),
              "online": 1,
              "working_day": str(aid)+" working_day",
              "city_id": city_id,
              "lat": str(driver_lat),
              "lng": str(driver_lng)}
    driver["orders"] = [generate_order(driver["city_id"], i) for i in order_ids]
    return driver

def generate_driver_info(generate_driver_info_opt):
    res = {"status": 1, "error":""}
    driver_num = generate_driver_info_opt["driver_num"]
    orders = generate_driver_info_opt["driver_orders"]
    city_id = generate_driver_info_opt["city_id"]
    order_ids = generate_driver_info_opt["order_ids"]
    res["data"] = []
    for i in range(driver_num):
        res["data"].append(generate_driver(i+1, city_id, order_ids[:orders[i]]))
        order_ids = order_ids[orders[i]:]
    return res

def generate_order_info(generate_order_info_opt):
    res = {"status":1, "error":""}
    cities = generate_order_info_opt["city_ids"]
    res["data"] = [generate_order(cities[i], generate_order_info_opt["order_ids"][i]) for i in range(len(generate_order_info_opt["order_ids"]))]
    return res

def generate_data(driver_info_opt, order_info_opt):
    total_orders = sum(driver_info_opt["driver_orders"]) + order_info_opt["order_num"]
    orders = [100000+i for i in range(total_orders)]
    driver_info_opt["order_ids"] = orders[order_info_opt["order_num"]:]
    order_info_opt["order_ids"] = orders[:order_info_opt["order_num"]]
    return generate_driver_info(driver_info_opt), generate_order_info(order_info_opt)

def each_city_query(city_id):
    random.seed(0)
    url = "https://api.luci.ai/multipickup"
    url = "https://fkhpu3p8db.execute-api.us-east-1.amazonaws.com/Prod/multipickup"
    driver_num = 5
    #city_id = 3611
    generate_driver_info_opt = {"driver_num":driver_num,"city_id":city_id, "driver_orders":[3 for i in range(driver_num)]}
    order_num = 1
    generate_order_info_opt = {"order_num":order_num,"city_ids":[city_id for i in range(order_num)]}
    driver_who_reject = 0 #range is [0, driver_num) which driver rejects new order
    optpara={'timeLimit': 24}
    objection = {"travel_time": 10,  "load_balance": 10}
    rejection = [{"driver_id": 1, "order_id": 2}] # reject id is not in driver list
    message = {}
    message["driverinfo"],message["orderinfo"] = generate_data(generate_driver_info_opt,generate_order_info_opt)
    rejection = [{"driver_id": message["driverinfo"]["data"][driver_who_reject]["aid"], "order_id": message["orderinfo"]["data"][0]["id"]}] # reject id is not in driver list
    message["optpara"] = optpara
    message["city_id"]=city_id
    message["rejection"]=rejection
    message["objection"]=objection
    #message["driverinfo"]=""
    headers = {"x-api-key": "D1mHMAoU4u76E8tnItABc8R3WnpufFHr7PZpSu5g"}
    #headers = {}
    resp = requests.post(url, headers=headers, data=json.dumps(message,indent=4))
    print(json.dumps(message,indent=4))
    print(str(city_id))
    print(resp.json())

if __name__ == "__main__":
    message = {"city_id":3141}
    driver_num = 5
    order_num = 0
    new_order_num = 1
    driver_info_opt = {"driver_num":driver_num,"city_id":3141, "driver_orders":[order_num for i in range(driver_num)]}
    order_info_opt = {"order_num":new_order_num,"city_ids":[3141]}
    message["driverinfo"],message["orderinfo"] = generate_data(driver_info_opt, order_info_opt)
    file_name = "fake_" + str(driver_num) + "_" + str(order_num)+"_"+str(new_order_num)+".json"
    with open("./input_jsons/"+file_name,"w+") as f:
        f.write(json.dumps(message))
    pprint(message)
