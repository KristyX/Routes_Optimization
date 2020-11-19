from utils import *
from pprint import pprint
import json

if __name__ == "__main__":
    for city_id in city_id_list:
        df = connect_to_mysql(city_id)
        df_process = process_data(df, city_id)
        mapper_path = XGB_PATH + str(city_id) + XGB_MAPPER_SUFFIX
        init_online_mapper(df_process, mapper_path)

