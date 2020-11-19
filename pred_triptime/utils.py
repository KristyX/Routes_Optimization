import sklearn
from sklearn_pandas import DataFrameMapper
from constants import *
import os
import pickle
import numpy as np
from benchmark import *


def convert_traveltime_to_min(travel_time):
    time_arr = travel_time.split(' ')
    min = 0
    for i in range(len(time_arr)):
        if time_arr[i] == 'min':
            min += float(time_arr[i - 1])
        elif time_arr[i] == 'h':
            min += float(time_arr[i - 1]) * 60
    if min > 30:
        return 0
    return min


def convert_distance_to_km(distance):
    dis_arr = distance.split(' ')
    km = 0
    for i in range(len(dis_arr)):
        try:
            if dis_arr[i] == 'km':
                if "," not in dis_arr[i - 1]:
                    if float(dis_arr[i - 1]) < 100:
                        km += float(dis_arr[i - 1])
            elif dis_arr[i] == 'm':
                km += float(dis_arr[i - 1]) / 1000
        except Exception:
            print(dis_arr)
            pass
    return km


def convert_timestamp_to_numeric(timestamp):
    hour = timestamp[0:2]
    min = timestamp[2:4]
    time = float(hour) + float(min) / 60
    return time


def convert_log_time_to_minute(log_time):
    time_minute = list(map(lambda x: np.expm1(x), log_time))
    return time_minute


def init_data_mapper(df, pickle_file):
    mapper = DataFrameMapper([
        (['start_lat'], sklearn.preprocessing.MinMaxScaler()),
        (['start_lng'], sklearn.preprocessing.MinMaxScaler()),
        (['end_lat'], sklearn.preprocessing.MinMaxScaler()),
        (['end_lng'], sklearn.preprocessing.MinMaxScaler()),
        # (['distance'], sklearn.preprocessing.MinMaxScaler()),
        (['month'], sklearn.preprocessing.MinMaxScaler()),
        (['day'], sklearn.preprocessing.MinMaxScaler()),
        (['weekday'], sklearn.preprocessing.MinMaxScaler()),
        (['time'], sklearn.preprocessing.MinMaxScaler()),

        ('log_trip_time', None),
        ('triptime', None),
    ], df_out=True)

    data_mapper = np.round(mapper.fit_transform(df.copy()).astype(np.double), 3)
    if check_local_file_exist(pickle_file):
        os.remove(pickle_file)
    with open(pickle_file, "wb") as f:
        pickle.dump(mapper, f)
    print("Fitting: ", type(mapper))
    return data_mapper


def init_online_mapper(df, mapper_path):
    mapper = DataFrameMapper([
        (['start_lat'], sklearn.preprocessing.MinMaxScaler()),
        (['start_lng'], sklearn.preprocessing.MinMaxScaler()),
        (['end_lat'], sklearn.preprocessing.MinMaxScaler()),
        (['end_lng'], sklearn.preprocessing.MinMaxScaler()),
        (['month'], sklearn.preprocessing.MinMaxScaler()),
        (['day'], sklearn.preprocessing.MinMaxScaler()),
        (['weekday'], sklearn.preprocessing.MinMaxScaler()),
        (['time'], sklearn.preprocessing.MinMaxScaler()),
    ], df_out=True)

    data_mapper = np.round(mapper.fit_transform(df.copy()).astype(np.double), 3)
    if check_local_file_exist(mapper_path):
        os.remove(mapper_path)
    with open(mapper_path, "wb") as f:
        pickle.dump(mapper, f)
    print("Fitting: ", type(mapper))
    return data_mapper


def check_local_file_exist(fname):
    return os.path.isfile(fname)


def remove_local_file(fname):
    exist = os.path.exists(fname)
    if exist:
        os.remove(fname)


def show_accuracy(df_result):
    df_result['diff'] = abs(df_result['pred_time'] - df_result['triptime'])
    df_outlier = df_result[df_result['diff'] > 5]
    num_test = float(len(df_result))
    perc_1min = len(df_result[df_result['diff'] <= 1]) / num_test
    perc_2min = len(df_result[df_result['diff'] <= 2]) / num_test
    perc_5min = len(df_result[df_result['diff'] <= 5]) / num_test
    perc_outlier = len(df_outlier) / num_test
    print(len(df_outlier))

    print("====================Trip Time Performance====================")
    print(df_outlier.sort_values(by=['diff'], ascending=False))
    print("Number of test samples: ", num_test)
    print("Error rate within 1mins: " + '%.2f%%' % (perc_1min * 100))
    print("Error rate within 2mins: " + '%.2f%%' % (perc_2min * 100))
    print("Error rate within 5mins: " + '%.2f%%' % (perc_5min * 100))
    print("Outliers ( >5min ): " + '%.2f%%' % (perc_outlier * 100))



