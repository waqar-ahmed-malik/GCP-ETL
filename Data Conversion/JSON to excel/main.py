import json
from json_parser import JSON_to_excel

source_file = '.\\Source.json'
destination_file = '.\\Target.xlsx'
main_sheet_name = 'Main'
JSON_to_excel(source_file, destination_file, main_sheet_name)