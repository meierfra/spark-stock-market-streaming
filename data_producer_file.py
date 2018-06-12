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
GLOB = "20180606-*.json"
#FILE_PATTERN = ".*/\d+-(1[789]|2)\d+\.json$"
FILE_PATTERN = ".*\.json"
#GLOB = "*.json"


def norm_quote_key(key):
    return key.split(" ")[1]


def quote_proc(quote):
    quote = {norm_quote_key(k): v for k, v in quote.items()}
    quote['price'] = float(quote.get('price'))
    #quote['volume'] = float(quote.get('volume'))
    return quote


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


for filename in sorted(glob.glob(COLLECT_DIR + "/" + GLOB)):
    if re.match(FILE_PATTERN, filename):
        with open(filename) as f:
            data = json.load(f)
            print("extract and dump data from: {}".format(filename))
            quotes = extract_quotes(data)
            dump_quotes(quotes)
        time.sleep(1)
