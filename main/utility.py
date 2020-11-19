import json
import logging
import constants
from botocore.exceptions import ClientError
import os
import pickle
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log_info(timecost):
    logger.info(json.dumps(timecost))


def log_error(code, msg, id, instance="lambda"):
    logging.error(json.dumps({"instance": instance, "global_id": id, "statusCode": code, "errorMsg": msg}))


def response_access(status_code, message, UniqueTimeId, result):
    json_hc = {"statusCode": status_code,
               "body": json.dumps({"global_id": UniqueTimeId, "message": message, "status": status_code})}
    if result == "success":
        logger.info(json_hc)
    elif result == "fail":
        log_error(status_code, message, UniqueTimeId)
    return json_hc


def upload_file_use_firehose(stream_name, content):
    try:
        constants.firehose_client.put_record(DeliveryStreamName=stream_name,
                                             Record={'Data': content})
    except ClientError as e:
        logging.error(e)


def response_return(status_code, message, UniqueTimeId, charge, result=[]):
    json_result = {"statusCode": status_code,
                   "body": json.dumps(
                       {"global_id": UniqueTimeId, "message": message, "status": status_code, "results": result})}
    json_save = {"global_id": UniqueTimeId, "message": message, "status": status_code, "results": result,
                 "charged": charge}
    if result == []:
        log_error(status_code, message, UniqueTimeId)
        upload_file_use_firehose(constants.FIREHOSE_ERR, json.dumps(json_save))
    else:
        upload_file_use_firehose(constants.FIREHOSE_RES, json.dumps(json_save))
    return json_result


def load_region_hist(filename, d_type):
    filepath = '/tmp/' + filename + '.pickle'
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as handle:
            hist = pickle.load(handle)
    else:
        if d_type == 'list':
            hist = []
        if d_type == 'dict':
            hist = {}

    return hist


def save_region_hist(filename, obj):
    filepath = '/tmp/' + filename + '.pickle'
    with open(filepath, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)


def time_cost(start_time):
    timecost = time.time() - start_time
    return timecost
