from django.shortcuts import render
from django.http.response import JsonResponse
import pickle
import pandas as pd
import json
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
from comet_utils.database.comet_historian import CometHistorian
from comet_utils.database.comet import Comet
from comet_utils.backtester.backtester import Backtester as bt
from comet_utils.processor.processor import Processor as p
import os
from dotenv import load_dotenv
load_dotenv()
mongouser = os.getenv("MONGOUSER")
mongokey = os.getenv("MONGOKEY")
comet_historian = CometHistorian(mongouser,mongokey)
comet = Comet(True,mongouser,mongokey)
@csrf_exempt
def backtestView(request):
    try:
        comet_historian.cloud_connect()
        comet.cloud_connect()
        key = comet_historian.retrieve("historian_key").iloc[0]["key"]
        if request.method == "GET":
            complete = {}
        elif request.method == "DELETE":
            complete = {}
        elif request.method == "UPDATE":
            complete = {}
        elif request.method == "POST":
            info = json.loads(request.body.decode("utf-8"))
            if info["key"] == key:
                start = datetime.strptime(info["start"],"%Y-%m-%d")
                end = datetime.strptime(info["end"],"%Y-%m-%d")
                for key in info.keys():
                    if key in ["req","signal","retrack_days"]:
                        info[key] = int(info[key])
                comet_historian.cloud_connect()
                comet_historian.store("backtest_request",pd.DataFrame([info]))
                prices = comet.retrieve("coinbase_prices")
                prices = p.column_date_processing(prices)
                trades = bt.backtest(start,end,info,prices)
                analysis = bt.analyze(trades,prices)
                complete = {"trades":trades.to_dict("records"),"analysis":analysis.to_dict("records")}
            else:
                complete = {"trades":[],"errors":"incorrect key"}
        else:
            complete = {}
        comet.disconnect()
        comet_historian.disconnect()
    except Exception as e:
        complete = {"trades":[],"errors":str(e)}
    return JsonResponse(complete,safe=False)