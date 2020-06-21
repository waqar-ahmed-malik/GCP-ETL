import requests
import urllib


def get_access_token(username:str, password:str) -> str:
  endpoint = "https://api.brandwatch.com/oauth/token"
  params = {
    'username': username,
    'password': password,
    'grant_type': 'api-password',
    'client_id': 'brandwatch-api-client'
    }
  
  response = requests.get(endpoint, params=params).json()
  return urllib.parse.quote(response['access_token'], safe='')


def get_mentions_data(username:str, password:str, mentions_date: str, project_id: str, query_id: str, batch_size: int=5000) -> dict:
  access_token = get_access_token(username, password)  
  endpoint = "https://api.brandwatch.com/projects/{}/data/mentions.json".format(project_id)
  resultsPage = 0
  resultsTotal = 0
  data = list()
  params = {
    'queryId': query_id,
    'startDate': mentions_date,
    'endDate': mentions_date,
    'pageSize': batch_size,
    'access_token': access_token,
    'page': resultsPage
    }
  while resultsPage * batch_size <= resultsTotal:
    response = requests.get(endpoint, params=params).json()
    resultsPage = response.get('resultsPage')
    resultsTotal = response.get('resultsTotal')
    data.extend(response.get('results'))
