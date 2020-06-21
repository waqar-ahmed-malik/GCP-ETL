import requests


def get_access_token(client_id:str, client_secret:str, refresh_token:str) -> str:
    request_data = {
        'client_id' : client_id, 	
        'client_secret' : client_secret,
        'refresh_token' : refresh_token,
        'grant_type' : 'refresh_token'
        }

    endpoint = 'https://www.googleapis.com/oauth2/v4/token'
    token_data = requests.post(endpoint, data=request_data).json()
    return "Bearer {}".format(token_data['access_token'])