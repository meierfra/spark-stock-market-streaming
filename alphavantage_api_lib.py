'''
Created on 5 Jun 2018

@author: meierfra
'''


def get_api_key():
    API_KEY_FILE = './alphavantage_api_key.txt'
    try:
        with open(API_KEY_FILE) as f:
            return f.read()
    except Exception as e:
        print ("ERROR reading API key file '{}': Please create the file with a valid API key as content".format(API_KEY_FILE))
        print (e)
