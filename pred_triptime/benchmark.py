from formula import get_trip_time_use_formula
import pymysql.cursors
import pandas as pd
from utils import *
from constants import *
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import datetime
from sklearn.model_selection import train_test_split
from xgboost_model import train_xgboost_with_gpu, predict_use_xgboost
import pickle


def connect_to_mysql(city):
    connection = pymysql.connect(host='192.168.0.128',
                                 user='foodhwy',
                                 password='raptors2019',
                                 db='foodhwy_pro')
    # TODO: LIMIT 1000
    sql = "SELECT * FROM foodhwy_pro.triptime_distance WHERE city_id =" + str(city) + " ORDER BY `timestamp` DESC"
    distances_table = pd.read_sql(sql, con=connection)
    # print(distances_table)

    return distances_table


def process_data(df_table, city_id):
    df_table['distance'] = list(map(lambda x: convert_distance_to_km(x), df_table['distance'].values))
    df_table = df_table[df_table["distance"] != 0]
    df_table['triptime'] = list(map(lambda x: convert_traveltime_to_min(x), df_table['triptime'].values))
    df_table = df_table[df_table["triptime"] != 0]
    df_table['month'] = list(map(lambda x: float(x.split('-')[1]), df_table['timestamp'].values))
    df_table['day'] = list(map(lambda x: float(x.split('-')[2]), df_table['timestamp'].values))
    df_table["date"] = list(
        map(lambda x: x.split("-")[0] + "-" + x.split("-")[1] + "-" + x.split("-")[2], df_table["timestamp"].values))
    df_table['weekday'] = list(
        map(lambda x: float(
            datetime.date(int(x.split('-')[0]), int(x.split('-')[1]), int(x.split('-')[2])).weekday()) + 1,
            df_table['date'].values))
    df_table['time'] = list(map(lambda x: x.split('-')[-1][0:4], df_table['timestamp'].values))
    df_table['time'] = list(map(lambda x: convert_timestamp_to_numeric(x), df_table['time'].values))
    df_table.to_csv('/var/luci/foodhwy/data_file/triptime/triptime_process_data.csv', index=False)

    # Normalization
    df_table['log_trip_time'] = np.log1p(df_table['triptime'])
    return df_table


def generate_train_test_data(df):
    df = df.sample(frac=1).reset_index(drop=True)
    y = df['log_trip_time']
    x = df[TRAIN_COLS]
    x_train_all, x_test, y_train_all, y_test = train_test_split(x.values, y.values, test_size=0.1, random_state=42,
                                                                shuffle=True)
    x_train, x_valid, y_train, y_valid = train_test_split(x_train_all, y_train_all, test_size=0.2, random_state=42,
                                                          shuffle=True)
    return x_train, y_train, x_valid, y_valid, x_test, y_test


def get_xgb_result(x_train, y_train, x_valid, y_valid, x_test, df_result, city_id):
    model_path = XGB_PATH + str(city_id) + XGB_MODEL_SUFFIX
    train_xgboost_with_gpu(x_train, y_train, x_valid, y_valid, model_path)
    xgb_log_time = predict_use_xgboost(x_test, model_path)
    pred_time = convert_log_time_to_minute(xgb_log_time)
    df_result['pred_time'] = pred_time
    # df_result.to_csv(DATA_PATH + 'xgb_result.csv', index=False)
    show_accuracy(df_result)
    return df_result


def get_formula_result(df_data, x_test, df_result):
    df_test = pd.DataFrame(data=x_test, columns=TRAIN_COLS)
    df = df_test[['start_lat', 'start_lng', 'end_lat', 'end_lng']]
    df = inverse_transform(df_data, df, 'start_lat')
    df = inverse_transform(df_data, df, 'start_lng')
    df = inverse_transform(df_data, df, 'end_lat')
    df = inverse_transform(df_data, df, 'end_lng')
    # print(df)

    modDfObj = df.apply(get_trip_time_use_formula, axis=1)
    df_result['formula_pred_time'] = modDfObj
    df_result.to_csv(DATA_PATH + 'formula_result.csv', index=False)
    return df_result


def inverse_transform(df_data, df, col, scale_max=1, scale_min=0):
    max_value = max(df_data[col].values)
    min_value = min(df_data[col].values)
    df[col] = (df[col] - scale_min) / (scale_max - scale_min) * (max_value - min_value) + min_value
    return df


if __name__ == "__main__":

    for city_id in city_id_list:
        # file_path = "/var/luci/foodhwy/data_file/triptime/foodhwy_trip_time.csv"
        df = connect_to_mysql(city_id)
        # df.to_csv(file_path, index=False)
        # df = pd.read_csv(file_path)
        df_process = process_data(df, city_id)
        mapper_path = XGB_PATH + str(city_id) + XGB_MAPPER_SUFFIX
        df_mapper = init_data_mapper(df_process, mapper_path)
        # df.to_csv('/var/luci/foodhwy/data_file/triptime/foodhwy_mapper_data.csv', index=False)
        # mapper_file = '/var/luci/foodhwy/data_file/triptime/foodhwy_mapper_data.csv'
        # origin_data_file = '/var/luci/foodhwy/data_file/triptime/triptime_process_data.csv'
        # df_mapper = pd.read_csv(mapper_file)
        # df_data = pd.read_csv(origin_data_file)
        x_train, y_train, x_valid, y_valid, x_test, y_test = generate_train_test_data(df_mapper)

        df_result = pd.DataFrame([])
        trip_time = convert_log_time_to_minute(y_test)
        df_result['triptime'] = trip_time
        '''
        xgboost
        '''
        df_result = get_xgb_result(x_train, y_train, x_valid, y_valid, x_test, df_result, city_id)

    '''
    formula
    '''
    # df_result = get_formula_result(df_data, x_test, df_result)
    # df_result.to_csv(DATA_PATH + 'results_file_624.csv', index=False)
