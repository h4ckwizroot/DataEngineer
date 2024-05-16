import pyodbc
import pandas as pd

# -----------------------------------------
driver_name = ''  
server_name = ''
database_name = ''
username = ''
password = ''
# -----------------------------------------

# this connection string can be found from the azure sql database connection string
connection = 'Driver={' + driver_name + '};Server=tcp:' + server_name + '.database.windows.net,1433;Database=' + database_name + ';Uid=' + username + ';Pwd=' + password + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# Create a new PYODBC Connection Object 
cnxn: pyodbc.Connection = pyodbc.connect(connection)

# Create a new Cursor Object from the connection 
crsr: pyodbc.Cursor = cnxn.cursor()

# Define SQL Query 
select_sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES"

# Execute SQL Query 
crsr.execute(select_sql)
rows = crsr.fetchall()
df = pd.DataFrame.from_records(rows, columns=[column[0] for column in crsr.description])

print(df)

# Close the connection once we are done 
crsr.close()
cnxn.close()
