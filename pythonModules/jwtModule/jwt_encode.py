import jwt
from datetime import datetime
from datetime import timedelta


key = 'Fz2BsVnMxxPuyII60i9/Jw=='

payload = {
        'username': 'username',
        'exp': datetime.utcnow() + timedelta(hours=1),
        'Other Info': 'Other Info'
        }

access_token = jwt.encode(payload, key).decode('utf-8')

