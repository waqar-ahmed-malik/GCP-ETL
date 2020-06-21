import pysftp


def read_file(file_path: str, username: str, password: str, host: str) -> list:
    data = []
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(SFTP_HOST, username=username, password=password, cnopts=cnopts) as sftp:
        File_Flag = sftp.isfile(file_path)
        if File_Flag == True:
            with sftp.open(file_path, mode='rb') as f:
                for line in f:
                    data.append(line)
    return data


def write_file(data: list, file_path: str, username: str, password: str, host: str):
    csv_data = list()
    column_names = [key for key, value in data[0].items()]
    csv_data.append(','.join(column_names))
    for row in data:
        record = list()
        for key, value in row.items():
            if key in column_names:
                record.append(str(value))
        csv_data.append(','.join(record))

    csv_data = '\n'.join(csv_data)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(SFTP_Host, username=SFTP_username, password=SFTP_password, cnopts=cnopts) as sftp:
        with sftp.open('{}'.format(file_path), "w") as f:
            f.write(csv_data)
