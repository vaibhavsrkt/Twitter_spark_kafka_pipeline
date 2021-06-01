import tweepy
import json
import os
from datetime import datetime
from kafka import KafkaProducer
import json 


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:json.dumps(x).encode('utf-8'))

print('Producer is Ready')

class MyStreamListener(tweepy.StreamListener):
    def on_error(self, status_code):
        if status_code == 420:
            print("------------ limit exceeds ------------")
            return False

    def on_status(self, status):
        tweet = dict()
        tweet['id'] = status.id_str
        tweet['time'] = status.timestamp_ms
        tweet_text = ""
        try:
            tweet_text = status.extended_tweet.full_text
        except:
            tweet_text = status.text
        tweet['text'] = tweet_text
        if len(tweet_text) > 0:
            print(tweet)
            producer.send(topic = 'seedhe_maut', value=tweet)


if __name__ == '__main__':
    consumer_key = "dThfb5tINHu9Zv4eDklLv4lJS"
    consumer_secret = "FeFnNNOQvuH7dHDclOTlZCO09fp3HYqy1W5OLXWUOyhimf4GZ0"
    access_token = "793361802700660736-XMunSPTgNgcaTB8i2i8HZcliW75nluI"
    access_token_secret = "DYMOiZKbZGMce9d5M6KhMNfffUEVn8MGiSvz2XsB3VXJX"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    myStream.filter(track=['the', 'i', 'to', 'a', 'and', 'is', 'in', 'it', '&'], languages=['en'])

