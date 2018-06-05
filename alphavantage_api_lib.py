'''
Created on 5 Jun 2018

@author: meierfra
'''


import requests


def get_api_key():
    API_KEY_FILE = './alphavantage_api_key.txt'
    try:
        with open(API_KEY_FILE) as f:
            return f.read().strip()
    except Exception as e:
        print ("ERROR reading API key file '{}': Please create the file with a valid API key as content".format(API_KEY_FILE))
        raise e


def get_batch_stock_quotes(symbols=[], interval='1min', datatype='json'):
    API_KEY = get_api_key()
    query_url = 'https://www.alphavantage.co/query?datatype=json&function=BATCH_STOCK_QUOTES' \
                + '&symbols=' + ",".join(symbols) + '&interval=' + interval + '&datatype=' + datatype \
                + '&apikey=' + API_KEY
    r = requests.get(query_url)
    # print("status: {}".format(r.status_code))
    r.raise_for_status()
    return r.text
