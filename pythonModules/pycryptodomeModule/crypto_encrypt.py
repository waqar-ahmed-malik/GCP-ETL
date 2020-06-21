from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from base64 import b64decode
from base64 import b64encode


data = '8BKXpMtAuC5r+MbScaRXmLgcA7F9BygMUdjVX1XZA4k8k6YO4hm3q+1zwn5iglHZbJUjiPJwpwLDtySuSFncDQ=='
key = 'Fz2BsVnMxxPuyII60i9/Jw=='
iv = 'o7o1BJBAw06Pug64bS9OSA=='

iv = b64decode(iv)
data = data.encode('utf-8')
key = key.encode('utf-8')

cipher = AES.new(key, AES.MODE_CBC, iv=iv)
encrypted_data = cipher.encrypt(pad(data, AES.block_size))
encrypted_data = b64encode(encrypted_data).decode('utf-8')
print(encrypted_data)


