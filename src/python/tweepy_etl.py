import google.auth
from google.cloud import storage

import variables as v
import pandas as pd
import tweepy


def run_twitter_etl():
    # Google auth
    credentials, project = google.auth.default()
    client = storage.Client(credentials=credentials)

    # Twitter auth
    auth = tweepy.OAuthHandler(v.access_key, v.access_secret)
    auth.set_access_token(v.consumer_key, v.consumer_secret)

    # Creating API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                               count=200,
                               include_rts=False,
                               tweet_mode='extended')

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         "text": text,
                         "favorite_count": tweet.favorite_count,
                         "retweet_count": tweet.retweet_count,
                         "created_at": tweet.created_at}

        tweet_list.append(refined_tweet)

    dataframe = pd.DataFrame(tweet_list)

    bucket = client.get_bucket(v.bucket_name)
    bucket.blob('data/twitter_data.csv') \
          .upload_from_string(dataframe.to_csv(), 'text/csv')
