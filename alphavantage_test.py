'''
Created on 4 Jun 2018

@author: meierfra
'''

import requests
import json
import alphavantage_api_lib as api


API_KEY = api.get_api_key()
if not API_KEY:
    print("Could not get API key -> exit")
    exit(1)


SYMBOLS = ["TSLA", "AAPL", "MSFT", "MCD", "NKE", "PFE", "FB", "GOOGL", "GS", "LMT"]
# Tesla
# Apple
# Microsoft
# MC Donalds
# Nike
# Facebook
# Google
# Goldman Sachs
# Lockheed Martin
#


def simple_test():
    query_url = 'https://www.alphavantage.co/query?datatype=json&function=TIME_SERIES_INTRADAY&symbol=' + SYMBOLS[0] + '&interval=1min&apikey=' + API_KEY
    r = requests.get(query_url)
    print("status: {}".format(r.status_code))
    resp_json = r.json()
    print(json.dumps(resp_json, indent=1))
    # print(json.dumps(list(resp_json.get("Time Series (1min)").items())[0], indent=1))


def batch_test():
    query_url = 'https://www.alphavantage.co/query?datatype=json&function=BATCH_STOCK_QUOTES&symbols=' + ",".join(SYMBOLS) + '&interval=1min&apikey=' + API_KEY
    r = requests.get(query_url)
    print("status: {}".format(r.status_code))
    if (r.status_code == 200):
        resp_json = r.json()
        print(json.dumps(resp_json, indent=1))
    else:
        print(r.text)


simple_test()
batch_test()
