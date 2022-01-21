from django.shortcuts import render
from django.http.response import JsonResponse
import pickle
import pandas as pd
import json
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
from comet_utils.database.comet import Comet
from comet_utils.backtester.backtester import Backtester as bt
import os
from dotenv import load_dotenv
load_dotenv()
mongouser = os.getenv("MONGOUSER")
mongokey = os.getenv("MONGOKEY")
comet = Comet(True,mongouser,mongokey)
@csrf_exempt
def backtestView(request):
    try:
        comet.cloud_connect()
        key = comet.retrieve("historian_key").iloc[0]["key"]
        if request.method == "GET":
            complete = {}
        elif request.method == "DELETE":
            complete = {}
        elif request.method == "UPDATE":
            complete = {}
        elif request.method == "POST":
            print(request.body)
            print(request.body.decode("utf-8"))
            info = json.loads(request.body.decode("utf-8"))
            print(info)
            if info["key"] == key:
                start = datetime.strptime(info["start"],"%Y-%m-%d")
                end = datetime.strptime(info["end"],"%Y-%m-%d")
                for key in info.keys():
                    if key in ["req","signal","retrack_days"]:
                        info[key] = int(info[key])
                comet.cloud_connect()
                comet.store("backtest_request",pd.DataFrame([info]))
                prices = comet.retrieve("coinbase_prices")
                trades = bt.backtest(start,end,info,prices)
                complete = {"trades":trades.to_dict("records")}
            else:
                complete = {"trades":[],"errors":"incorrect key"}
        else:
            complete = {}
        comet.disconnect()
    except Exception as e:
        complete = {"trades":[],"errors":str(e)}
        print(str(e))
    return JsonResponse(complete,safe=False)