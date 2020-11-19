from math import sin, cos, sqrt, atan2, radians


def cal_distance(lat1, lng1, lat2, lng2):
    R = 6373.0
    lat1 = radians(float(lat1))
    lon1 = radians(float(lng1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(lng2))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return sqrt(2) * R * c


def get_trip_time_use_formula(row, speed=20):
    lat1 = row['start_lat']
    lng1 = row['start_lng']
    lat2 = row['end_lat']
    lng2 = row['end_lng']

    trip_time_hour = cal_distance(lat1, lng1, lat2, lng2) / float(speed)
    trip_time_minute = trip_time_hour * 60

    return trip_time_minute
