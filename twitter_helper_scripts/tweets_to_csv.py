import os
import json
import time
import math

HASHTAGS = ['tesla', 
            'apple',
            'microsoft',
            'mcdonalds',
            'nike',
            'pfizer',
            'facebook',
            'alphabet',
            'goldmansachs',
            'lockheadmartin']

TWEET_DIR = 'tweet_data'
CSV_SEP = ";"

with open("tweets.csv", "w") as out:
    header = CSV_SEP.join(["timestamp", "hashtag", "cnt", "followers_count", "log_followers_count"])
    out.write(header + "\n")

    for filename in sorted(os.listdir(TWEET_DIR)):
        filename = os.path.join(TWEET_DIR, filename)
        #print("processing tweet " + filename)

        with open(filename, "r") as f:
            tweet = json.load(f)
            hashtag = "n/a"
            hashtag_list = []
            hashtag_list.append(tweet['entities']['hashtags'])
            if 'extended_tweet' in tweet.keys():
                hashtag_list.append(tweet['extended_tweet']['entities']['hashtags'])
            if 'retweeted_status' in tweet.keys():
                hashtag_list.append(tweet['retweeted_status']['entities']['hashtags'])
                if 'extended_tweet' in tweet['retweeted_status'].keys():
                    hashtag_list.append(tweet['retweeted_status']['extended_tweet']['entities']['hashtags'])
            if 'quoted_status' in tweet.keys():
                if 'extended_tweet' in tweet['quoted_status']:
                    hashtag_list.append(tweet['quoted_status']['extended_tweet']['entities']['hashtags'])
            for ht_list in hashtag_list:
                for ht in  [tag['text'].lower() for tag in ht_list]:
                    if ht in HASHTAGS:
                        hashtag = ht
                        #print ("  found hashtag " + hashtag)
                        break

            follower_cnt = tweet['user']['followers_count']
            timestamp = tweet['created_at']

            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(timestamp,'%a %b %d %H:%M:%S +0000 %Y'))

            log_val = 0
            if follower_cnt > 0:
                log_val = math.log(follower_cnt)
            out_str = CSV_SEP.join([ts, hashtag, "1", str(follower_cnt), str(log_val)])
            out.write(out_str + "\n")
            #print("found: " + timestamp + " - " + hashtag + " - " + " - " + str(follower_cnt))


