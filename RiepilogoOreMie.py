from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse
import sqlite3 as sl
from datetime import datetime

# Connection details
collection_name = "Orari"
db_path = "./DataBase.db"

con = sl.connect(db_path)
cursor = con.cursor()

try:
    # Define the path to your Access database
    database_path = r"\\192.168.1.133\Aggiornamenti\Renzo\timb online 4-3-24\Timbrature-240304.accdb"

    # Create a connection string with proper URL encoding for the path
    params = urllib.parse.quote_plus(
        f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={database_path}"
    )
    connection_string = f"access+pyodbc:///?odbc_connect={params}"

    # Create an engine
    engine = create_engine(connection_string)
    print("Connection successful!")

    # Parameter for the query
    codDip_value = 1008

    # Write your SQL query to fetch data with a parameter placeholder
    query = "SELECT CodDip_Timb, TimeIn_Timb, TimeOut_Timb, OreLav_Timb FROM Timbrature_T WHERE CodDip_Timb = :codDip_value"
    query2 = "SELECT * FROM "

    # Execute the query with the parameter using SQLAlchemy's text() function
    with engine.connect() as connection:
        result = connection.execute(text(query), {"codDip_value": codDip_value})

        # Fetch all rows from the executed query
        rows = result.fetchall()

        # Get column names from the result set
        columns = result.keys()

    # Convert the result set to a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Execute the query with the parameter using SQLAlchemy's text() function
    with engine.connect() as connection:
        result = connection.execute(text(query2), {"codDip_value": codDip_value})

        # Fetch all rows from the executed query
        rows2 = result.fetchall()

        # Get column names from the result set
        columns2 = result.keys()

    # Convert the result set to a pandas DataFrame
    dp = pd.DataFrame(rows2, columns=columns2)

    # Convert TimeIn_Timb and TimeOut_Timb to datetime
    df["TimeIn_Timb"] = pd.to_datetime(df["TimeIn_Timb"])
    df["TimeOut_Timb"] = pd.to_datetime(df["TimeOut_Timb"])

    # Sort DataFrame by index (optional)
    df = df.sort_index(ascending=False)

    # Convert DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    df.to_csv("output.csv", index=False)
    dp.to_csv("output2.csv", index=False)

    con.execute("DROP TABLE IF EXISTS Orari;")

    df.to_sql(collection_name, con, if_exists="replace", index=False)

    df["month_year"] = df["TimeIn_Timb"].dt.to_period("M").astype(str)
    # print("df[month_year]", df["month_year"])
    unique_months = df["month_year"].unique()
    # print("unique_month", unique_months)

    def create_monthly_table(month, df):
        Orari = f"data_{month.replace('-', '_')}"
        # print("Orari ", Orari)
        df.to_sql(Orari, con, if_exists="replace", index=False)

    for month in unique_months:
        if month != "NaT":
            month_data = df[df["month_year"] == month]
            create_monthly_table(month, month_data)

    # Remove the erroneous query since we already created the Orari table and populated it
    # cursor.execute(f"SELECT OreLav_Timb from {collection_name} where CodDip_Timb = ?", (codDip_value,))

except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()
