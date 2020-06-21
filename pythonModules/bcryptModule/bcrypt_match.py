import bcrypt


encrypted_data = '$2b$12$RHaJBY6zbewLnaINKrWFMO5YwahsSJs2EcCHbGHCtSA2B.Mi6cwsa'
encrypted_data = encrypted_data.encode('utf-8')
data = 'Nc!wBf5h^5br3'
data = data.encode('utf-8')
if bcrypt.checkpw(data, encrypted_data):
    print("match")