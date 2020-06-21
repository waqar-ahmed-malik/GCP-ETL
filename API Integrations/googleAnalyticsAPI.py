import requests
from googleAPI import get_access_token
import math


def get_ga_ids(client_id: str, client_secret: str, refresh_token: str, view_name: str) -> int:
    access_token = get_access_token(client_id, client_secret, refresh_token)
    endpoint = 'https://www.googleapis.com/analytics/v3/management/accountSummaries'
    headers = {
        'Authorization': access_token
    }
    response = requests.get(endpoint, headers=headers).json()
    for account in accounts_summary['items']:
        for web_property in account['webProperties']:
            for view in web_property['profiles']:
                if view['name'] == view_name:
                    return {
                        'account_id': account['id'],
                        'property_id': web_property['id'],
                        'view_id': view['id']
                    }
                else:
                    return None


def get_goals(client_id: str, client_secret: str, refresh_token: str, view_name: str) -> dict:
    ga_ids = get_ga_ids(client_id, client_secret, refresh_token, view_name)
    access_token = get_access_token(client_id, client_secret, refresh_token)
    endpoint = 'https://www.googleapis.com/analytics/v3/management/accounts/{}/webproperties/{}/profiles/{}/goals'.format(
        ga_ids['account_id'], ga_ids['property_id'], ga_ids['view_id'])

    return requests.get(endpoint, headers=headers).json()


def get_data(client_id: str, client_secret: str, refresh_token: str, view_name: str, data_date: str, dimensions: list, metrics=list) -> dict:
    data = []
    view_id = get_ga_ids(client_id, client_secret,
                         refresh_token, view_name)['view_id']
    access_token = get_access_token(client_id, client_secret, refresh_token)
    endpoint = 'https://www.googleapis.com/analytics/v3/data/ga'

    headers = {
        'Authorization': access_token
    }

    params = {
        'ids': 'ga:{}'.format(view_id),
        'start-date': data_date,
        'end-date': data_date,
        'samplingLevel': 'HIGHER_PRECISION',
        'include-empty-rows': 'false',
        'metrics': ','.join(metrics),
        'dimensions': ','.join(dimensions)
    }

    response = requests.get(endpoint, params=params, headers=headers).json()

    for i in range(math.ceil(response['totalResults']/1000)):
        params.setdefault('start-index', (i*1000)+1)
        data.append(requests.get(
            endpoint, params=params, headers=headers).json())
