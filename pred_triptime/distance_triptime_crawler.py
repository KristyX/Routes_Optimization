import pymysql
from distance_crawler import DistanceCrawler
import random
from multiprocessing import cpu_count, Pool
import datetime
import warnings
import time

warnings.filterwarnings("ignore")
# encrypted
db = pymysql.connect('192.168', 'fffyy', 'raptors2019', 'fffppp')
cursor = db.cursor()
core = int(cpu_count() * 0.7)  # 8 cores / 12
city_id = [3141, 3142, 3149, 3152, 3606, 3609, 3610, 3611, 3612, 3613, 3614, 3615, 3616, 3617, 3618, 3619]


def get_position_samples():
    sql_customer = "SELECT DISTINCT city_id, customer_lat, customer_lng FROM foodhwy_pro.customer_info"
    sql_driver = "SELECT DISTINCT `order`.city_id, driver_lat, driver_lng FROM " \
                 "(SELECT city_id, order_id FROM foodhwy_pro.order_info) AS `order` " \
                 "INNER JOIN " \
                 "(SELECT DISTINCT driver_lat, driver_lng, order_id FROM foodhwy_pro.driver_order_status) AS `dos`" \
                 "USING (order_id)"
    sql_shop = "SELECT DISTINCT `order`.city_id, shop_lat, shop_lng FROM " \
               "(SELECT city_id, shop_id FROM foodhwy_pro.order_info) AS `order` " \
               "INNER JOIN " \
               "(SELECT DISTINCT shop_id, shop_lat, shop_lng FROM foodhwy_pro.shop_info) AS `dos`" \
               "USING (shop_id)"
    cursor.execute(sql_customer)
    customer_position = list(cursor.fetchall())
    cursor.execute(sql_driver)
    driver_position = list(cursor.fetchall())
    cursor.execute(sql_shop)
    shop_position = list(cursor.fetchall())

    driver_loc_dic = convert_sqllist_to_dic(driver_position)
    fix_loc_dic = convert_sqllist_to_dic(list(set(shop_position + customer_position)))

    return fix_loc_dic, driver_loc_dic


def convert_sqllist_to_dic(sql_list):
    city_loc_dic = {}
    for item in sql_list:
        if item[0] not in city_loc_dic.keys():
            city_loc_dic[item[0]] = [[item[1], item[2]]]
        else:
            city_loc_dic[item[0]].append([item[1], item[2]])

    return city_loc_dic


def count_location_by_city(loc_dic):
    count_dic = {}
    for city in loc_dic.keys():
        count_dic[city] = len(loc_dic[city])
    return count_dic


def random_sample(city):  # fix_dic, driver_dic
    if len(fix_dic[city]) >= 180:
        fix_sample = random.sample(fix_dic[city], 180)
    else:
        fix_sample = random.sample(fix_dic[city], len(fix_dic[city]))
    if len(driver_dic[city]) >= 150:
        driver_sample = random.sample(driver_dic[city], 150)
    else:
        driver_sample = random.sample(driver_dic[city], len(driver_dic[city]))
    samples = fix_sample + driver_sample
    return samples


def get_location_input(loc_list):
    loc_input = str(loc_list[0]) + "," + str(loc_list[1])
    return loc_input


def multiprocess_crawler():
    pool = Pool(processes=core)
    pool.map(auto_crawler, city_id)
    pool.close()
    pool.join()


def insert_into_mysql(sql, val):
    cursor = db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return


def insert_result_into_mysql(time_dist_list, city, start_position, end_postion):
    triptime = time_dist_list[0]
    distance = time_dist_list[1]
    start_lat = start_position.split(",")[0]
    start_lng = start_position.split(",")[1]
    end_lat = end_postion.split(",")[0]
    end_lng = end_postion.split(",")[1]
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S%f")
    print(timestamp)
    val = (city, start_lat, start_lng, end_lat, end_lng, distance, triptime, timestamp)

    sql = "INSERT IGNORE INTO foodhwy_pro.triptime_distance (city_id, start_lat, start_lng, end_lat, end_lng, distance," \
          "triptime, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    insert_into_mysql(sql, val)
    return


# suppose for one city
def auto_crawler(city):
    try:
        dist_crawler = DistanceCrawler()
        city_sample = random_sample(city)
        input_sample = list(map(lambda x: get_location_input(x), city_sample))
        for start_loc in input_sample:
            for end_loc in input_sample:
                try:
                    if start_loc != end_loc:
                        # TODO: only process valid and correct lat & lng
                        time_dist_list = dist_crawler.get(start_loc, end_loc)
                        # ['17 min', '8.0 km']
                        insert_result_into_mysql(time_dist_list, city, start_loc, end_loc)
                    else:
                        print(start_loc, end_loc)
                except Exception as e:
                    errmsg = type(e).__name__ + " " + str(e)
                    print(errmsg)
                    pass
    except Exception as e:
        errmsg = type(e).__name__ + " " + str(e)
        print(errmsg)
        pass

    return


if __name__ == "__main__":
    while True:
        try:
            fix_dic, driver_dic = get_position_samples()
            multiprocess_crawler()
        except Exception as e:
            errmsg = type(e).__name__ + " " + str(e)
            print(errmsg)
            pass
        time.sleep(60 * 15)
