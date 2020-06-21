import pyodbc


username = 'Username'
password = 'Password'
database = 'Database'
server = 'Server URL without https'
db_driver = '{ODBC Driver 17 for SQL Server}'
port = 1433

conn = pyodbc.connect('DRIVER={};SERVER={};PORT=1433;DATABASE={};UID={};PWD={}'.format(db_driver, server, database,
                                                                                       username, password))
query = "SELECT * FROM SCHEMA.TABLE"
cursor = conn.cursor()
cursor.execute(query)
# cursor.commit()               ''' In case we need to execute DML or DDL. '''
for row in cursor.fetchall():
    column = row[0]
