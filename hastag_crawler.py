import tweepy
import datetime
import csv
import pytz
try:
    import json
except ImportError:
    import simplejson as json
import sqlite3
import sys
import os


hashtah_=sys.argv[1]


##########>>> SET TWITTER API KEYS PARAMETERS
ACCESS_TOKEN = 'xxxxxxxxxxxxx'
ACCESS_SECRET = 'xxxxxxxxxxxxx'
CONSUMER_KEY = 'xxxxxxxxxxxxx'
CONSUMER_SECRET = 'xxxxxxxxxxxxx'
#########>>> SET TWEEPY HANDLER AND FEED PARAMETERS TO IT
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True)


##########>>> DEFINE SQLITE CONNECTION AND CONNECT
conn = sqlite3.connect('tweets.db')
c = conn.cursor()

##########>>> Drop Previous Table if it's necessary; Uncomment only if you want change the table structure
c.execute('''DROP TABLE IF EXISTS tweets_db''')

##########>>> CREATE TABLE FOR TWEETS
# Create table  IF NOT EXISTS
c.execute(
    '''CREATE TABLE IF NOT EXISTS tweets_db  
(
tw_id text  NOT NULL PRIMARY KEY, 
tw_usr_id text,
tw_user_name text, 
tw_user_sc_name text, 
tw_datetime text,
tw_text text, 
tw_raw_json text,
tw_hashtags text,
tw_orig_id TEXT,
tw_in_reply_to_screen_name TEXT,
tw_in_reply_to_user_id_str TEXT,
tw_is_quote_status number 
)'''
          )

##########>>> START RETRIEVING TWEETS BY GETTING A CURSOR FROM TWEEPY
##########>>> AND INSERT THEM IN DB
cnt=0;
while True:
    crsr=tweepy.Cursor(api.search,q=hashtah_,count=100000000,since="2017-08-16",tweet_mode='extended').items()
    for tweet in crsr:
        vals=[
            tweet.id_str,
            tweet.user.id_str,
            tweet.user.name,
            tweet.user.screen_name,
            str(tweet.created_at),
            tweet.full_text if (  not hasattr(tweet, 'retweeted_status') ) else tweet.retweeted_status.full_text,
            str(tweet._json),
            json.dumps(tweet.entities) if ( not hasattr(tweet, 'retweeted_status') ) else json.dumps(tweet.retweeted_status.entities),
            tweet.id_str if ( not hasattr(tweet, 'retweeted_status') ) else tweet.retweeted_status.id_str,
            tweet.in_reply_to_screen_name,
            tweet.in_reply_to_user_id_str,
            tweet.is_quote_status
        ]
        try:
            res= c.execute("INSERT INTO tweets_db VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",vals)
            conn.commit()
        except:
            # print(sys.exc_info()[0])
            continue

        cnt = cnt + 1
        # os.system('cls')
        # print(cnt)



