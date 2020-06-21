import re


phone_numbers = list()
names = list()
addresses = list()
with open("data.txt", "r") as f:
    lines = f.readlines()
    name_pattern = re.compile(r'^[a-zA-Z]+\s[a-zA-Z]+')
    phone_number_pattern = re.compile(r'^[0-9]+[-][0-9]+[-][0-9]+')
	url_pattern = re.compile(r'https?://(www\.)?(\w+)(\.\w+)')
	email_pattern = re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
	
	emails = '''CoreyMSchafer@gmail.com
	corey.schafer@university.edu
	corey-321-schafer@my-work.net
	'''
	matches = email_pattern.finditer(emails)

for match in matches:
    print(match)

    for line in lines:
        for name in name_pattern.findall(line):
            names.append(name)
        for phone_number in phone_number_pattern.findall(line):
            phone_numbers.append(phone_number)
		
		
print(phone_numbers)
print(names)
