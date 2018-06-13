import pandas as pd

data = pd.read_csv('tweets.csv', sep=';', index_col='timestamp', parse_dates=True)
grouped_data = data.groupby("hashtag").resample("1H", how="sum")
grouped_data.to_csv("grouped_tweets.csv", sep=';')
