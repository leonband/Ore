import pyodbc
import sqlite3

# Specify the path to your Access database
access_db_path = (
    r"\\192.168.1.133\Aggiornamenti\Renzo\timb online 4-3-24\Timbrature-240304.accdb"
)

# Specify the path to your SQLite database
sqlite_db_path = "./StructureDB.db"


def get_column_names(database_path, table_names):
    """
    Retrieves column names for each table in the given list from an Access database.

    :param database_path: Path to the Access database file.
    :param table_names: List of table names to retrieve column names for.
    :return: A dictionary with table names as keys and lists of column names as values.
    """
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={database_path};"
    )

    table_columns = {}

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for table in table_names:
                query = f"SELECT * FROM [{table}] WHERE 1=0"
                cursor.execute(query)
                columns = [column[0] for column in cursor.description]
                table_columns[table] = columns
    except Exception as e:
        print(f"An error occurred: {e}")

    return table_columns


def create_sqlite_db(sqlite_db_path, table_columns):
    """
    Creates a SQLite database and imports the tables with their columns.

    :param sqlite_db_path: Path to the new SQLite database file.
    :param table_columns: Dictionary with table names as keys and lists of column names as values.
    """
    try:
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()
            for table, columns in table_columns.items():
                columns_def = ", ".join([f'"{col}" TEXT' for col in columns])
                create_table_query = (
                    f'CREATE TABLE IF NOT EXISTS "{table}" ({columns_def});'
                )
                cursor.execute(create_table_query)
            conn.commit()
    except Exception as e:
        print(f"An error occurred while creating SQLite DB: {e}")


def retrieve_table_names(access_conn_str):
    """
    Retrieves all table names from the Access database.

    :param access_conn_str: Connection string for the Access database.
    :return: List of table names.
    """
    table_names = []

    try:
        with pyodbc.connect(access_conn_str) as conn:
            cursor = conn.cursor()
            for row in cursor.tables():
                if row.table_type in ["TABLE", "LINK"]:
                    table_names.append(row.table_name)
    except Exception as e:
        print(f"An error occurred while retrieving table names: {e}")

    return table_names


def insert_table_names(sqlite_db_path, table_names):
    """
    Inserts the table names into a new table in the SQLite database.

    :param sqlite_db_path: Path to the SQLite database file.
    :param table_names: List of table names to insert.
    """
    try:
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Tables (
                TableName TEXT PRIMARY KEY
            )
            """
            cursor.execute(create_table_query)
            for table_name in table_names:
                cursor.execute(
                    "INSERT OR IGNORE INTO Tables (TableName) VALUES (?)", (table_name,)
                )
            conn.commit()
    except Exception as e:
        print(f"An error occurred while inserting table names: {e}")


# Main execution
access_conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={access_db_path};"
)

# Retrieve table names from Access
table_names = retrieve_table_names(access_conn_str)

# Insert table names into SQLite
insert_table_names(sqlite_db_path, table_names)

# Retrieve column names from Access
table_columns = get_column_names(access_db_path, table_names)

# Create SQLite database and import tables
create_sqlite_db(sqlite_db_path, table_columns)

# Print the table names to confirm
print("Table names retrieved and stored in SQLite:")
print(table_names)
