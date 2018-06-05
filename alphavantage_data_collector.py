#!/usr/bin/env python3

'''
Created on 5 Jun 2018

@author: meierfra
'''

import os
import datetime
import time
import alphavantage_api_lib as api


SYMBOLS = ["TSLA", "AAPL", "MSFT", "MCD", "NKE", "PFE", "FB", "GOOGL", "GS", "LMT"]
TIME_START = datetime.time(14, 0, 0)
TIME_STOP = datetime.time(23, 0, 0)


def dump_resp(data):
    COLLECT_DIR = './collected_data'
    try:
        os.mkdir(COLLECT_DIR)
    except FileExistsError:
        pass
    filename = COLLECT_DIR + "/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".json"
    with open(filename, 'w') as f:
        f.write(data)


def do_collect():
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d-%H%M%S")
    print(now_str + " ", end='')

    if now > datetime.datetime.combine(datetime.date.today(), TIME_STOP) \
            or now < datetime.datetime.combine(datetime.date.today(), TIME_START):
        print(' -> suspended')
    else:
        # run collection
        try:
            resp = api.get_batch_stock_quotes(SYMBOLS)
            # print()
            # print("-------------------")
            # print(resp)
            # print("-------------------")
            dump_resp(resp)
            print('-> OK')
        except RuntimeError as e:
            print(' -> ERROR ' + str(e))


while True:
    do_collect()
    time.sleep(60)
