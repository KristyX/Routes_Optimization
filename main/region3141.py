import datetime
import copy
from subprocess import TimeoutExpired
from utility import *
import opt_algorithm
import preprocess
import postprocess
from foodhwy_errors import *
import pandas as pd
from constants import *


def run_solution(logistics_params, start_time):
    mod_path = logistics_params["optpara"]["model_path"]
    timeout = logistics_params["optpara"]["timeLimit"] + 10
    results = []
    data = preprocess.produce_data(logistics_params)
    max_time = 0
    i = 1
    while len(results) < NUM_SOL and time.time() - start_time + max_time < TIME_LIMIT:
        s = time.time()
        # optimaize alogrithm
        with open(DAT_PATH, 'w') as f:
            dat = data["dat"] + preprocess.format_dat(data["pref"], "Pref")
            f.write(dat)
        opt_output = opt_algorithm.opt_alg(OPL_PATH, mod_path, DAT_PATH, timeout)
        solution = opt_output["opl"]
        if solution == '':
            raise NoSolutionError("No solution found caused by input data error.")
        result, assignment = postprocess.format_lambda_output(data, solution)
        if result == {}:
            break
        results.append(result)

        data["pref"] = preprocess.update_Pref(data["pref"], assignment[["order_id", "driver_id"]].values, 0)
        data["dataframes"]["reject_table"] = pd.concat([data["dataframes"]["reject_table"], assignment])
        f = time.time() - s
        if f > max_time:
            max_time = f
        i += 1
    return results


def lambda_handler(event, context):
    UniqueTimeId = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")
    try:
        start_time = time.time()
        req = json.loads(event["body"])
        # req = event
        # ***************************************************************************************
        # Health Check
        try:
            if preprocess.validate_health_check(req):
                return response_access(202, "Health Check OK", UniqueTimeId, "success")
        except Exception:
            return response_access(503, "Health Check Error", UniqueTimeId, "fail")
        # ***************************************************************************************
        if ("charged" in req) and req["charged"] == "True":
            charge = "True"
        else:
            charge = "False"
        if "global_id" not in req:
            req["global_id"] = UniqueTimeId
            preprocess.save_input_data_to_s3(req, UniqueTimeId)

        # ***************************************************************************************
        # Check input format and Preprocess
        try:
            errors = preprocess.check_schema_error(req)
            for error in errors:
                return response_return(400, "input error " + error.message, UniqueTimeId, charge)
            logistics_params = preprocess.preprocess(req)
        except KeyError as e:
            print(e)
            return response_return(400, "Required keys are missing. Please check standard input!", UniqueTimeId, charge)
        except ObjectionRangeError as e:
            return response_return(400, e.message, UniqueTimeId, charge)

        # ***************************************************************************************
        try:
            results = run_solution(logistics_params, start_time)
        except NoSolutionError as e:
            return response_return(400, e.message, UniqueTimeId, charge)
        except NoDriverError as e:
            return response_return(400, e.message, UniqueTimeId, charge)
        except NoOrderError as e:
            return response_return(400, e.message, UniqueTimeId, charge)
        except TravelTimeError as e:
            return response_return(400, e.message, UniqueTimeId, charge)
        except ValueError:
            return response_return(400, "Required values are incorrect!", UniqueTimeId, charge)
        except KeyError:
            return response_return(400, "Required values are not provided!", UniqueTimeId, charge)
        except OSError:
            return response_return(500, "Optimization's subprocess error: os error ", UniqueTimeId, charge)
        except TimeoutExpired:
            return response_return(500, "Optimization's subprocess error: timeout", UniqueTimeId, charge)
        if len(results) == 0:
            return response_return(400, "0 solution was found!", UniqueTimeId, charge, results)
        elif len(results) == 1:
            return response_return(200, "1 solution was found!", UniqueTimeId, charge, results)
        return response_return(200, str(len(results)) + " solutions were found!", UniqueTimeId, charge, results)
    except Exception as e:
        errmsg = type(e).__name__ + " " + str(e)
        print(errmsg)
        return response_return(500, errmsg, UniqueTimeId, "False")
