import openpyxl
import csv


source_path = '.\\Source.xlsx'
destination_path = '.\\Destination.csv'
sheet_name = 'Sheet1'
excelFile = openpyxl.load_workbook(source_path)
sheet = excelFile[sheet_name]
with open(destination_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for row in sheet.iter_rows():
        row = [cell.value for cell in row]
        writer.writerow(row)
excelFile.close()
