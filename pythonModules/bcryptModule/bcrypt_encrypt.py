import bcrypt
data = '$62ns&WwZs7zH'
data = data.encode('utf-8')

salt = bcrypt.gensalt()
encrypted_data = bcrypt.hashpw(data, salt)
encrypted_data = encrypted_data.decode('utf-8')
print(encrypted_data)