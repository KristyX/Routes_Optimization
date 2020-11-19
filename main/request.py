# -*- coding: utf-8 -*-
import requests
import time
import json
import datetime

city_id = [3141, 3142,  3149, 3152, 3606, 3609, 3610, 3611, 3612, 3613, 3615, 3616]



def callCity(id):
    city_id = id
    print(city_id)
    optpara = {'timeLimit': 6}
    objection = {'travel_time': 3, 'load_balance': 490}
    #rejection = {'driver_id': 6670, 'order_id': 2384171}
    driverinfo = {'status': 1, 'error': '', 'data': [
        {'aid': 6495, 'nickname': 'A-ALLAN', 'online': 1, 'working_day': '2019-07-31', 'city_id': 3142, 'lat': '43.57295165789421',
         'lng': '-79.65948762142678', 'orders': []},
        {'aid': 6670, 'nickname': 'A-ALLAN', 'online': 1, 'working_day': '2019-07-31', 'city_id': 3142, 'lat': '43.57295165787421',
         'lng': '-79.65948762143678', 'orders': []},
        {'aid': 6342, 'nickname': 'A-ALLAN', 'online': 1, 'working_day': '2019-07-31', 'city_id': 3142, 'lat': '43.57295165786421',
         'lng': '-79.65948762145678', 'orders': []}
    ]}

    orderinfo = {'status': 1, 'error': '', 'data': [{'id': 2408002, 'status': 10, 'city_id': 3142, 'created_at': 1564579917, 'cookingtime_set': 1800, 'cookingtime_result': 1564538580, 'customer_id': 792304, 'customer_lat': '43.2524653', 'customer_lng': '-79.8703712', 'distance': 1000, 'shop_id': 16735, 'shipping_amount': '5.00', 'subtotal': '16.95', 'data': '{"contact":{"name":"陈璟","tel":4373458659,"short_tel":4373458659,"addr":"135 James Street South, Hamilton, ON, Canada"},"customer_order_count":58,"shop_daily_count":4,"payment":{"type":"cashpay"},"shop":{"name":"爱至味黄焖鸡Chicken Zone Hamilton"},"city_tax_rate":0.13,"time_zone":"America/Toronto","reward_deduct":{"rating_reward_amount":[0,0,0,0,0],"rating_deduct_amount":0,"late_deduct_amount":0,"qd_late_deduct_amount":0},"free_driver_shipping_fee":0,"comments":{"user":"","extra":[{"time":1564081245,"desc":"状态变更：已确认 by 爱至味黄焖鸡","admin_id":7093}]},"price":{"subtotal":16.95,"grand_total":25.79,"shipping_amount":5,"total_paid":null,"tax":2.85},"platform_rate":85,"driver_collection_enabled":true,"addition":{"shipping_fee":0,"shipping_draw_fee":0,"subsidy_enabled":false,"grab_draw_fee":1}}', 'shop_lat': '43.255371', 'shop_lng': '-79.871071', 'shop_name': '爱至味黄焖鸡Chicken Zone Hamilton', 'shop_tel': '9055236666', 'shop_addr': '25 Main St W, Hamilton, ON L8P 1H1'}]}

    #orderinfo = {}
    #driverinfo = {}

    message = '{' + '"objection"' + ":" + json.dumps(objection) + "," + '"optpara"' + ":" + json.dumps(optpara) + ',' + '"city_id"' + ':' + json.dumps(city_id) + ',' + '"driverinfo"' + ':' + json.dumps(driverinfo) + ',' + '"orderinfo"' + ':' +json.dumps(orderinfo) + '}'
    headers = {"x-api-key": "D1mHMAoU4u76E8tnItABc8R3WnpufFHr7PZpSu5g"}
    url = "https://bcjw45lnib.execute-api.us-west-1.amazonaws.com/Prod/multipickup"
    print(datetime.datetime.now())
    resp = requests.post(url, headers=headers, data=message)

    print()
    return resp.text

def main():
    for i in range(12):
        log = callCity(city_id[i])
        print(log)
main()




