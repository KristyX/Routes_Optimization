import json
import os
import datetime
import time
import ast
import boto3
import requests
from configparser import ConfigParser

config = ConfigParser()
config.read('lucix.conf')
env = config.get("Environ", "env")
region = config.get("Environ", str(env) + "_region")

luci_key = config.get("Environ", "luci_key")
foodhwy_key = config.get("Environ", "foodhwy_key")

charged_city = [3613, 3612, 3611, 3142, 3610, 3619]
whole_city = [3141, 3142, 3149, 3152, 3606, 3609, 3610, 3611, 3612, 3613, 3614, 3615, 3616, 3617, 3618, 3619, 3620,
              3621, -1]


def get_region_map(source):
    api_key = {}
    region_map = {}
    for city_id in whole_city:
        api_url = config.get("RegionMap", str(env) + "_" + str(city_id))
        if source == "luci":
            api_key = {"x-api-key": config.get("Environ", "luci_key")}
        elif source == "foodhwy":
            if city_id in charged_city:
                api_key = {"x-api-key": config.get("Environ", "charge_region_key")}
            else:
                api_key = {"x-api-key": config.get("Environ", "free_region_key")}
        region_map[city_id] = {"url": api_url, "headers": api_key}
    return region_map


def lambda_handler(event, context):
    UniqueTimeId = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")
    try:
        notes = ast.literal_eval(event["body"])
        if 'x-api-key' in event["headers"]:
            if event["headers"]["x-api-key"] == luci_key:
                notes["source"] = "luci"
                region_map = get_region_map("luci")
                print("luci's request")
            if event["headers"]["x-api-key"] == foodhwy_key:
                notes["source"] = "foodhwy"
                region_map = get_region_map("foodhwy")
                if "city_id" in notes:
                    region = int(notes["city_id"])
                    if region in charged_city:
                        notes["charged"] = "True"
                    elif region not in charged_city:
                        notes["charged"] = "False"
                print("foodhwy's request")
        if 'city_id' in notes:
            region = int(notes["city_id"])
            if region not in region_map:
                region = -1
        else:
            region = -1
        notes = json.dumps(notes)
        resp = requests.post(region_map[region]["url"], headers=region_map[region]["headers"], data=notes)
        message = resp.text
        return {"body": message}
    except Exception as e:
        errmsg = type(e).__name__ + " " + str(e)
        print(errmsg)
        return {"statusCode": 500,
                "body": json.dumps(
                    {"global_id": UniqueTimeId, "message": "Main API Error.", "status": 500, "results": []})}
