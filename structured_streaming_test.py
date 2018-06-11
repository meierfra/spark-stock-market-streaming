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
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("stock_struct_streaming").getOrCreate()
print("spark version:" + spark.version)

inputPath = "./stream_data_in"
df = spark.read.option("multiline", "true").json("./data-template.json")
# df.printSchema()
# json_schema = df.schema
# print(json_schema)
json_schema = StructType().add("timestamp", TimestampType()).add("symbol", StringType()).add("price", DoubleType()).add("volume", StringType())
print(json_schema)

streamingInputDF = spark.readStream.option("multiline", "true").schema(json_schema).json(inputPath)
print("isStreaming: {}".format(streamingInputDF.isStreaming))


quotesStreamQuery = streamingInputDF \
    .where("symbol = 'TSLA'").select("*") \
    .writeStream \
    .format("memory") \
    .queryName("quotestream") \
    .outputMode("append")\
    .start()

quotesDF = spark.sql("SELECT timestamp, symbol, price FROM quotestream ORDER BY timestamp")


# statStreamQuery = streamingInputDF \
#     .where("symbol = 'TSLA'") \
#     .groupby("symbol").agg(max("timestamp").alias('actual_timestamp'),
#                            avg("price").alias("price_avg"),
#                            max("price").alias("price_max"),
#                            min("price").alias("price_min")) \
#     .writeStream \
#     .format("memory") \
#     .queryName("statstream") \
#     .outputMode("complete")\
#     .start()
#
# statisticsDF = spark.sql("SELECT *, price_max-price_min as price_diff FROM statstream")


windowedStreamQuery = streamingInputDF \
    .where("symbol = 'TSLA'") \
    .groupBy("symbol", window("timestamp", "3600 seconds", "600 seconds")) \
    .agg(max("timestamp").alias("timestamp_max"),
         min("timestamp").alias("timestamp_min"),
         avg("price").alias("price_avg"),
         max("price").alias("price_max"),
         min("price").alias("price_min")) \
    .orderBy(desc("window")) \
    .writeStream \
    .format("memory") \
    .queryName("windowedstream") \
    .outputMode("complete")\
    .start()

windowedDF = spark.sql("SELECT * FROM windowedstream")


# joinedDF = quotesDF.join(statisticsDF, 'symbol') \
#     .select("timestamp", "symbol", "price", "price_avg", "price_min", "price_max", "price_diff") \
#     .orderBy(desc("timestamp"))


joinedDF = quotesDF.join(windowedDF, expr('quotestream.symbol = windowedstream.symbol AND quotestream.timestamp = windowedstream.timestamp_max')) \
    .select("timestamp", "quotestream.symbol", "price", "price_avg", "price_min", "price_max") \
    .orderBy(desc("timestamp"))


# while True:
for _ in range(10):
    quotesDF.show(10)
    # statisticsDF.show(10)
    windowedDF.show(10, False)
    joinedDF.show(10)

    timestampMax = joinedDF.select(max('timestamp')).collect()[0][0]
    print(timestampMax)
    if timestampMax:
        cutoffTime = timestampMax - datetime.timedelta(hours=5)
        outputDF = joinedDF.where("timestamp > '{}'".format(cutoffTime))
        # outputDF = joinedDF
        # outputDF.show()

        outputPandasDF = outputDF.toPandas()
        outputPandasDF.index = outputPandasDF['timestamp']
        del(outputPandasDF['timestamp'])
        # print(outputPandasDF)
        outputPandasDF[['price', 'price_avg', 'price_max', 'price_min']].plot()
        plt.pause(0.1)

    time.sleep(10)

# outputPandasDF[['price', 'price_avg', 'price_max', 'price_min']].plot()
input("FINISHED (press enter)")
# plt.show()
