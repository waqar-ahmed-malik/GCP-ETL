import requests


client_id = '29b38609-d698-4e68-b5f6-aabf35b61ff1'
client_secret ='K]wjGnW-=N112/HrYAXfc6P0A_5Qllts'
tenant_id = '5b86146a-b40f-4315-850b-c5cd36003cca'
display_name = 'Waqar Ahmed'


def get_access_token(client_id:str, client_secret:str, tenant_id:str) -> str:
    endpoint = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': 'https://graph.microsoft.com/.default'
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(endpoint, data=data, headers=headers).json()
    return f"{response['token_type']} {response['access_token']}"


def get_user_id(display_name: str, client_id:str, client_secret:str, tenant_id:str) -> str:
    token = get_access_token(client_id, client_secret, tenant_id)
    headers = {'Authorization': token}
    endpoint = 'https://graph.microsoft.com/v1.0/users'
    response = requests.get(endpoint, headers=headers).json()
    return ''.join([user['id'] for user in response['value'] if user['displayName'] == display_name])



