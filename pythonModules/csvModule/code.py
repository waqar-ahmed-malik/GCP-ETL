import csv


with open("Read.csv", "r", encoding="utf8") as csv_read:
    csv_reader = csv.DictReader(csv_read)
    fieldnames = list()
    for line in csv_reader:
        for key, value in line.items():
            if key not in fieldnames:
                fieldnames.append(key)
    csv_read.seek(0)
    with open("Write.csv", "w", newline='') as csv_write:
        csv_writer = csv.DictWriter(csv_write, fieldnames=fieldnames, )
        for line in csv_reader:
            csv_writer.writerow(line)
