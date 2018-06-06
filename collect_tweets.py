import tweepy
import json
import logging
import time

LOG="collect_tweets.log"

logging.basicConfig(
        filename=LOG,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')


logger = logging.getLogger('collect_tweets')
logger.setLevel(logging.DEBUG)

logger.info("---- starting ----")


# removed keys, because two programs with same keys running in parallel
# does not work.
CONSUMER_KEY = 'xyz'
CONSUMER_SECRET = 'xyz'
OAUTH_TOKEN = 'xyz'
OAUTH_TOKEN_SECRET = 'xyz'

HASHTAGS = ['#tesla', 
            '#apple',
            '#Microsoft',
            '#mcdonalds',
            '#nike',
            '#pfizer',
            '#facebook',
            '#alphabet',
            '#goldmansachs',
            '#lockheadmartin']

TWEET_DIR = 'tweet_data/' 

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        id = status.id
        logger.debug("Processing tweet" + str(id))
        filename = TWEET_DIR + str(id) + str(status.entities['hashtags'])
        with open(TWEET_DIR + str(id), 'w') as f:
            json.dump(status._json, f)

    def on_error(self, status_code):
        if status_code == 420:
            logger.error("!!! encountered 420. Aborting.")
            return False
        else:
            logger.error("!!! UNKNOWN ERROR " + str(status_code))
            return False

myStreamListener = MyStreamListener()
last_try_time = 0

while True:
    logger.info("---- start fetching ----")
    current_time = time.time()
    if current_time-last_try_time < 60*15:
        logger.warn("retrying too fast. Sleeping for 15min.")
        time.sleep(60*15)
    try:
        myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
        myStream.filter(track=HASHTAGS, async=False)
    except IOError as ex:
        logger.error('I just caught the exception: %s' % ex)
        sleep(60)

