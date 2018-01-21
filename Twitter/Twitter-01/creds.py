# -*- coding: utf-8 -*-

import json

import tweepy

from Storage import Storage

def get_tweepy_api():
    """return tweepy api object with proper authentication"""

    consumer_key = ''
    consumer_secret = ''

    access_token = ''
    access_token_secret = ''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth_handler=auth, wait_on_rate_limit=False, wait_on_rate_limit_notify=False)

    return api
