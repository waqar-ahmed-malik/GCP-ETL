import jwt
from datetime import datetime
from datetime import timedelta


key = 'Fz2BsVnMxxPuyII60i9/Jw=='
access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6IndoaXRlemlwIiwiZXhwIjoxNTcyOTY4NjAwLCJjbGllbnRfY29kZSI6IldaIn0.1iRXI8SJup5T-wxZUcNtSGizNrxLBap89oOuafJPHg4'

token_data = jwt.decode(access_token.encode('utf-8'), key)
expires_at = datetime.fromtimestamp(token_data['exp'])

# Condition to check for expiry (expires_at - datetime.now()).seconds > 0