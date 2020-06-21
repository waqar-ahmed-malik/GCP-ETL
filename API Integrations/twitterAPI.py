import requests
import base64
from datetime import datetime


def get_access_token(consumer_key: str, consumer_secret: str) -> str:
    consumer_combination = "{}:{}".format(consumer_key, consumer_key)
    encoded_consumer_combination = base64.b64encode(
        consumer_combination.encode('utf-8'))
    endpoint = 'https://api.twitter.com/oauth2/token'
    params = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Authorization': 'Basic {}'.format(encoded_consumer_combination.decode('utf-8'))
    }
    response = requests.post(endpoint, params=params, headers=headers).json()
    return "Bearer {}".format(response['access_token'])


def get_lifetime_post_data(consumer_key: str, consumer_secret: str, twitter_handle: str) -> list:
    data = list()
    endpoint = "https://api.twitter.com/1.1/statuses/user_timeline.json"
    params = {
        'screen_name': twitter_handle,
        'count': 200,
        'tweet_mode': 'extended'
    }
    headers = {'Authorization': get_access_token(
        consumer_key, consumer_secret)}
    for i in range(16):
        tweets = requests.get(
            tweet_endpoint, headers=headers, params=params).json()
        data.extend(tweets)
        params.setdefault('max_id': tweets[-1]['id'])
