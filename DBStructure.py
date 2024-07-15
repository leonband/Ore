import pyodbc
import sqlite3

# Step 1: Read table names from Access and create tables in SQLite

# Specify the path to your Access database
access_db_path = r"\\192.168.1.133\Aggiornamenti\Renzo\timb online 4-3-24\Timbrature-240304.accdb"

# Specify the path to your SQLite database
sqlite_db_path = "./StructureDB.db"

# Create a connection string for the Access database
access_conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={access_db_path};"
)

# Connect to the Access database
access_conn = pyodbc.connect(access_conn_str)
cursor = access_conn.cursor()

# List to store table names
table_names = []

# Get the list of all tables and linked tables
for row in cursor.tables():
    if row.table_type in ["TABLE", "LINK"]:
        table_names.append(row.table_name)

# Connect to the SQLite database (or create it if it doesn't exist)
sqlite_conn = sqlite3.connect(sqlite_db_path)
sqlite_cursor = sqlite_conn.cursor()

# Create a new table in the SQLite database to store the table names
create_table_query = """
CREATE TABLE IF NOT EXISTS Tables (
    TableName TEXT PRIMARY KEY
)
"""
sqlite_cursor.execute(create_table_query)

# Insert the table names into the SQLite table
for table_name in table_names:
    sqlite_cursor.execute(
        "INSERT OR IGNORE INTO Tables (TableName) VALUES (?)", (table_name,)
    )

# Commit the changes and close the SQLite connection
sqlite_conn.commit()
sqlite_conn.close()

# Close the Access database connection
access_conn.close()

# Print the table names to confirm
print("Table names retrieved and stored in SQLite:")
print(table_names)
