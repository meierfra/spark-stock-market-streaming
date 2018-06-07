#!/usr/bin/env python3

'''
Created on 6 Jun 2018

@author: meierfra
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import time


spark = SparkSession.builder.appName("stock_struct_streaming").getOrCreate()
print("spark version:" + spark.version)

inputPath = "./stream_data_in"
# df = spark.read.option("multiline", "true").json("./data-template.json")
# df.printSchema()
# json_schema = df.schema
# print(json_schema)
json_schema = StructType().add("timestamp", TimestampType()).add("symbol", StringType()).add("price", StringType()).add("volume", StringType())
print(json_schema)

streamingInputDF = spark.readStream.option("multiline", "true").schema(json_schema).json(inputPath)
print("isStreaming: {}".format(streamingInputDF.isStreaming))

# windowedStream = streamingInputDF.groupBy(window("timestamp", "1 seconds", "1 seconds"))
# agregationsStream = windowedStream.agg(avg("price"))
# streamingQuery = agregationsStream \
#     .select("*") \
#     .writeStream \
#     .format("memory") \
#     .queryName("quotestream") \
#     .outputMode("complete")\
#     .start()


quotesStreamQuery = streamingInputDF \
    .where("symbol = 'TSLA'").select("*") \
    .writeStream \
    .format("memory") \
    .queryName("quotestream") \
    .outputMode("append")\
    .start()


statStreamQuery = streamingInputDF \
    .where("symbol = 'TSLA'") \
    .groupby("symbol").agg(avg("price").alias("price_avg"), max("price").alias("price_max"), min("price").alias("price_min")) \
    .writeStream \
    .format("memory") \
    .queryName("statstream") \
    .outputMode("complete")\
    .start()


quotesDF = spark.sql("SELECT timestamp, symbol, price from quotestream ORDER BY timestamp")
statisticsDF = spark.sql("SELECT *, price_max-price_min as price_diff FROM statstream")

joinedDF = quotesDF.join(statisticsDF, 'symbol') \
    .select("timestamp", "symbol", "price", "price_avg", "price_min", "price_max", "price_diff") \
    .orderBy(desc("timestamp"))


#windowDF = joinedDF.groupBy(window("timestamp", "10 minutes", "60 seconds")).count()

# for _ in range(100):
while True:
    # statisticsDF.show()
    # quotesDF.show()
    # joinedDF.show()

    timestampMax = quotesDF.select( max('timestamp') ).collect()[0][0]
    print(timestampMax)
    if timestampMax:
        cutoffTime = timestampMax - datetime.timedelta(minutes=20)
        joinedDF.where("timestamp > '{}'".format(cutoffTime)).show(40)

    time.sleep(5)
