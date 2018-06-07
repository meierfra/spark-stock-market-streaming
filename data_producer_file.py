#!/usr/bin/env python3

'''
Created on 6 Jun 2018

@author: meierfra
'''

import glob
import json
import os
import re
import time

COLLECT_DIR = './collected_data'
#GLOB = "*.json"
GLOB = "20180606-17*.json"
#GLOB = "*.json"


def norm_quote_key(key):
    return key.split(" ")[1]


def quote_proc(quote):
    return {norm_quote_key(k): v for k, v in quote.items()}


def extract_quotes(data):
    # print(data)
    quotes = data.get("Stock Quotes")
    quotes_proc = [quote_proc(q) for q in quotes]
    # for q in quotes_proc:
    #    print(q)
    return quotes_proc


def dump_quotes(quotes):
    DUMPDIR = './stream_data_in'
    try:
        os.mkdir(DUMPDIR)
    except FileExistsError:
        pass
    for q in quotes:
        timestamp_str = re.sub('[-:]', "", q['timestamp']).replace(" ", "-")
        filename = timestamp_str + "-" + q['symbol'] + ".json"
        # print(filename)
        with open(DUMPDIR + "/" + filename, 'w') as f:
            json.dump(q, f)


for filename in glob.glob(COLLECT_DIR + "/" + GLOB):
    with open(filename) as f:
        data = json.load(f)
        print("extract and dump data from: {}".format(filename))
        quotes = extract_quotes(data)
        dump_quotes(quotes)
    time.sleep(1)