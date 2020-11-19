import requests
import time
def my_request(url, headers, message, method):
    start = time.time()
    if method == "GET":
        resp = requests.get(url, headers=headers, params=message)
    elif method == "POST":
        resp = requests.post(url, headers=headers, data=message)
    end = time.time()
    return end-start, resp.json()