'''
Created on 6 Jun 2018

@author: meierfra
'''
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


sc = SparkContext("local[2]", "stock-streaming")

window = 1
ssc = StreamingContext(sc, window)

inputPath = "./stream_data_in"
stream = ssc.textFileStream(inputPath)

stream.pprint()

timeout = 50
try:
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
finally:
    ssc.stop(False)
