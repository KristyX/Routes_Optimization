import polyline
import requests
import math
import json
import numpy as np
from pprint import pprint

radius = 6371; # earth's mean radius in km

# loc1 and loc2 are list of str of lat,lon and travel_time is the time driver
# spent (in seconds), the function returns the lat, lon of the driver's
# new location, provided that he travels from loc1 to loc2 using
# the fastest route
def calculate_new_location(loc1, loc2, travel_time=0):
    # the api requires format in lon followed by lan
    r = requests.post("http://3.222.89.226:80/route/v1/driving/{},{}"
                      ";{},{}?alternatives=false&steps=true&annotations=true&geometries=polyline&overview=full&annotations=true".format(loc1[1],
                                                                                                                                        loc1[0],
    loc2[1], loc2[0]))
    if r.status_code == 200:
        data = json.loads(r.text)
        annotations = data['routes'][0]['legs'][0]['annotation']
        speed = annotations['speed']  # meters per second
        duration = annotations['duration']  # duration in seconds
        distance = annotations['distance']  # in meters
        polylines = data['routes'][0]['geometry']
        polylines_decode = polyline.decode(polylines)  # format in [(lat1, lon1), (lat2, lon2), ...]
        acc_time = 0
        prev = [float(loc1[0]),float(loc1[1])]
        for node, t, s in zip(polylines_decode[1:], duration, speed):
            tmp = acc_time
            cur = list(node)
            acc_time+=t
            if acc_time > travel_time:
                bearing = calculateBearing(prev, cur)
                distanceTravelled = (travel_time - tmp)*s #in meters
                distanceTravelled = distanceTravelled/1000 #in km
                intermediaryLocation = calculateDestinationLocation(prev, bearing, distanceTravelled)
                return [str(intermediaryLocation[0]), str(intermediaryLocation[1])]
            prev = cur
        return loc2
    else:
        print(r.reason)

# refer to https://stackoverflow.com/questions/32829266/interpolate-between-2-gps-locations-based-on-walking-speed

# Helper function to convert degrees to radians
def degToRad(deg):
    return (deg * math.pi / 180);

# Helper function to convert radians to degrees
def radToDeg(rad):
    return (rad * 180 / math.pi)

# Calculate the (initial) bearing between two points, in degrees
def calculateBearing(startPoint, endPoint):
    lat1 = degToRad(startPoint[0])
    lat2 = degToRad(endPoint[0])
    deltaLon = degToRad(endPoint[1]-startPoint[1])

    y = math.sin(deltaLon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(deltaLon)
    bearing = math.atan2(y, x)

    return (radToDeg(bearing) + 360) % 360

# Calculate the destination point from given point having travelled the given distance (in km), on the given initial bearing (bearing may vary
# before destination is reached
def calculateDestinationLocation(point, bearing, distance):
    distance = distance / radius
    bearing = degToRad(bearing)

    lat1 = degToRad(point[0])
    lon1 = degToRad(point[1])

    lat2 = math.asin(math.sin(lat1) * math.cos(distance) + math.cos(lat1) * math.sin(distance) * math.cos(bearing))
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(distance) * math.cos(lat1), math.cos(distance) - math.sin(lat1) * math.sin(lat2))
    lon2 = (lon2 + 3 * math.pi) % (2 * math.pi) - math.pi

    return (radToDeg(lat2), radToDeg(lon2))

def _calculate_driving_time(sources, destinations):
    location_str = ";".join(map(lambda x: str(x[1]) + "," + str(x[0]), sources))
    sources_idx = ";".join(map(str, list(range(len(sources)))))
    location_str += ";" + ";".join(map(lambda x: str(x[1]) + "," + str(x[0]), destinations))
    destinations_idx = ";".join(map(str, list(range(len(sources), len(sources) + len(destinations)))))
    url = "http://3.222.89.226:80/table/v1/driving/{}?sources={}&destinations={}".format(location_str,
                                                                                         sources_idx,
                                                                                         destinations_idx)
    r = requests.post(url)
    assert (r.status_code == 200)
    data = json.loads(r.text)
    travel_time = np.array(data["durations"])
    return travel_time

def calculate_driving_time(sources, destinations):
    """Return a 2D list of travel """
    travel_time = []
    for i in range(0,len(sources),100):
        i_row = []
        for j in range(0,len(destinations),100):
            i_sources = sources[i:i+100]
            j_destinations = destinations[j:j+100]
            i_j_travel_time = _calculate_driving_time(i_sources, j_destinations)
            i_row.append(i_j_travel_time)
        i_row = np.concatenate(i_row,axis=1)
        travel_time.append(i_row)
    travel_time = np.concatenate(travel_time,axis=0)
    travel_time = np.around(travel_time / 60)
    return travel_time

def get_trip_time_distance(loc1, loc2):
    r = requests.post("http://3.222.89.226:80/route/v1/driving/{},{}"
                      ";{},{}?alternatives=false&steps=true&annotations=true&geometries=polyline&overview=full&annotations=true".format(
        loc1[1],
        loc1[0],
        loc2[1], loc2[0]))
    assert(r.status_code == 200)
    data = json.loads(r.text)
    annotations = data['routes'][0]['legs'][0]['annotation']
    duration = annotations['duration']  # duration in seconds
    total_travel_time = sum(duration)
    distance = annotations['distance']
    total_distance = sum(distance)
    return total_travel_time, total_distance

def get_trip_nodes(loc1, loc2, cur_time):
    r = requests.post("http://3.222.89.226:80/route/v1/driving/{},{}"
                      ";{},{}?alternatives=false&steps=true&annotations=true&geometries=polyline&overview=full&annotations=true".format(
        loc1[1],
        loc1[0],
        loc2[1], loc2[0]))
    assert(r.status_code == 200)
    data = json.loads(r.text)
    annotations = data['routes'][0]['legs'][0]['annotation']
    # pprint(annotations)
    speed = annotations['speed']  # meters per second
    duration = annotations['duration']  # duration in seconds
    polylines = data['routes'][0]['geometry']
    polylines_decode = polyline.decode(polylines)  # format in [(lat1, lon1), (lat2, lon2), ...]
    res = []
    for node, t, s in zip(polylines_decode[1:], duration, speed):
        cur_time += t
        res.append({"drop_by":list(node),"drop_by_start":cur_time, "drop_by_end":cur_time})
    return res

if __name__ == "__main__":
    # calculate_new_location tester
    A = ['42.30147804725378', '-83.03311719426034']
    B = ['42.305974', '-83.059244']
    import time
    travel_time, distance = calculate_new_location(A, B, t)
    print(travel_time)

    #intermediaryLocation = calculate_new_location(A, B, t)
    #print("The intermediaryLocation is {}".format(intermediaryLocation))
    #calculate driving_time
    _calculate_driving_time([A],[B])


