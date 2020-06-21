import jwt
import pytz
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Util.Padding import unpad
from base64 import b64decode
from base64 import b64encode


encrypted_data = '8BKXpMtAuC5r+MbScaRXmLgcA7F9BygMUdjVX1XZA4k8k6YO4hm3q+1zwn5iglHZbJUjiPJwpwLDtySuSFncDQ=='
key = 'Fz2BsVnMxxPuyII60i9/Jw=='
iv = 'o7o1BJBAw06Pug64bS9OSA=='

iv = b64decode(iv)
encrypted_data = b64decode(encrypted_data)

cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv)
decrypted_data = unpad(cipher.decrypt(encrypted_data), AES.block_size)
decrypted_data = decrypted_data.decode('utf-8')
print(decrypted_data)