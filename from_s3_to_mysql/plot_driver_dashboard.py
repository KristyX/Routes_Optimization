import pandas as pd
from flask import Flask
from flask import render_template
from flask import request
import json
import boto3
from pprint import pprint
from flask_socketio import SocketIO, emit, send
from time import sleep
import pymysql

app = Flask(__name__)
socketio = SocketIO(app)
updated = False
first_connection = True
info_list = []
fhw_db = pymysql.connect()
rds_db = pymysql.connect()


def list_driver_profit_per_day(city_id, date):

    return


@app.route('/driver-dashboard', methods=['POST'])
def driver_dashboard():
    input_json = request.get_json(force=True)
    result = {}
    # ****************************************************
    # {"purpose": "list_driver_profit_per_day", "city_id": 3141, "date": "2019-09-01"}
    if input_json["purpose"] == "list_driver_profit_per_day":
        print()
    # ****************************************************
    # {"purpose": "plot_driver_info", "start_date": "2019-09-01", "end_date": "2019-09-30"}
    elif input_json["purpose"] == "plot_driver_info":
        print()

    return result


@app.route('/api-trial', methods=['POST'])
def api_trial():
    result = {
        "driver_id": 7471,
        "assigned_order_id": 2480662,
        "driver_location": [
            "44.62910215419132",
            "-63.57212681403027"
        ],
        "driver_start_time": 1571154653,
        "route_plan": [
            {
                "drop_by_type": "S",
                "order_id": 2480636,
                "drop_by": [
                    "44.640682",
                    "-63.578006"
                ],
                "drop_by_id": 16867.0,
                "drop_by_start_time": 1571154833,
                "drop_by_end_time": 1571154833,
                "drop_by_late_time": 0
            },
            {
                "drop_by_type": "NS",
                "order_id": 2480662,
                "drop_by": [
                    "44.635874",
                    "-63.574647"
                ],
                "drop_by_id": 16870,
                "drop_by_start_time": 1571154953,
                "drop_by_end_time": 1571154953,
                "drop_by_late_time": 0
            },
            {
                "drop_by_type": "NC",
                "order_id": 2480662,
                "drop_by": [
                    "44.6362897",
                    "-63.5780843"
                ],
                "drop_by_id": 827371,
                "drop_by_start_time": 1571155013,
                "drop_by_end_time": 1571155013,
                "drop_by_late_time": 1292820
            },
            {
                "drop_by_type": "C",
                "order_id": 2480636,
                "drop_by": [
                    "44.6589545",
                    "-63.6425583"
                ],
                "drop_by_id": 843705.0,
                "drop_by_start_time": 1571155553,
                "drop_by_end_time": 1571155553,
                "drop_by_late_time": 1291800
            }
        ]
    }
    return result


@app.route('/bill-module', methods=['POST'])
def bill_module():
    request_data = {"year_month": "2019-10"}
    year_month = request_data["year_month"]
    year_month_list = request_data["year_month"].split("-")
    year_month_str = str(year_month_list[0]) + str(year_month_list[1])
    time_str = year_month_str + "%"
    sql_sentence = "SELECT global_id, order_id, city_id FROM foodhwy_pro.request_order_from_s3 WHERE global_id LIKE '" + str(
        time_str) + "' and charged='True'"
    pprint(sql_sentence)
    df_bill = pd.read_sql(sql_sentence, con=fhw_db)
    print(df_bill)
    print(len(df_bill))
    df_bill.to_csv(str(year_month) + "_bill_details.csv", index=False)

    return df_bill


if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=8080)
    # socketio.run(app, host="0.0.0.0")
    bill_module()

# if __name__ == "__main__":
#     city_id = 3141
#     df = pd.read_sql('SELECT date, num_orders FROM mvp_driver_dashboard WHERE city_id=' + str(city_id), con=rds_db)
#     df_max = df.groupby(['date']).max()
#     df_min = df.groupby(["date"]).min()
#     df_med = df.groupby(["date"]).median()
#     df = df_max.merge(df_min, left_on="date", right_on="date", how="inner")
#     df = df.merge(df_med, left_on="date", right_on="date", how="inner")
#     df["date"] = df.index
