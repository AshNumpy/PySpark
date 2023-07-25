# Live stream tweets containiing word "Turkey"
# Use Spark Streaming to find top Hashtags every 10 secs

# Importing libraries
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# API Keys
consumer_key = 'YOUR CONSUMER KEY'
consumer_secret = 'YOUR CONSUMER SECRET'
access_token = 'TWITTER ACCESS TOKEN'
access_token_secret = 'TWITTER ACCESS TOKEN SECRET'


class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['Turkey'])
