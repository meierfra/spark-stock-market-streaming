#!/usr/bin/env python3

'''
Created on 4 Jun 2018

@author: meierfra
'''

import requests
import json
import alphavantage_api_lib as api


API_KEY = api.get_api_key()


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


def simple_test(symbol):
    query_url = 'https://www.alphavantage.co/query?datatype=json&function=TIME_SERIES_INTRADAY&symbol=' + symbol + '&interval=1min&apikey=' + API_KEY
    r = requests.get(query_url)
    print("status: {}".format(r.status_code))
    print(r.text)
    #resp_json = r.json()
    #print(json.dumps(resp_json, indent=1))
    #print(json.dumps(list(resp_json.get("Time Series (1min)").items())[0], indent=1))


def batch_test():
    print(api.get_batch_stock_quotes(SYMBOLS))


simple_test("FB")

# batch_test()
