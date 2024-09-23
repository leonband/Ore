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
    query = "SELECT * FROM Timbrature_T WHERE CodDip_Timb = :codDip_value"

    # Execute the query with the parameter using SQLAlchemy's text() function
    with engine.connect() as connection:
        result = connection.execute(text(query), {"codDip_value": codDip_value})

        # Fetch all rows from the executed query
        rows = result.fetchall()

        # Get column names from the result set
        columns = result.keys()

    # Convert the result set to a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Print initial DataFrame
    print("Initial DataFrame:")
    print(df.head())

    # Convert TimeIn_Timb and TimeOut_Timb to datetime, coerce errors to handle invalid dates
    df["TimeIn_Timb"] = pd.to_datetime(df["TimeIn_Timb"], errors="coerce")
    df["TimeOut_Timb"] = pd.to_datetime(df["TimeOut_Timb"], errors="coerce")

    # Print DataFrame after conversion to datetime
    print("DataFrame after datetime conversion:")
    print(df.head())

    # Handle NaT and None values by filling with a placeholder date
    df["TimeIn_Timb"].fillna(pd.Timestamp("1970-01-01 00:00:00"), inplace=True)
    df["TimeOut_Timb"].fillna(pd.Timestamp("1970-01-01 00:00:00"), inplace=True)

    # Verify no NaT or None values remain
    print("DataFrame after filling NaT and None values:")
    print(df.head())

    # Convert datetime columns to string format that SQLite can handle
    df["TimeIn_Timb"] = df["TimeIn_Timb"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["TimeOut_Timb"] = df["TimeOut_Timb"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Print DataFrame after converting to string format
    print("DataFrame after converting to string format:")
    print(df.head())

    # Sort DataFrame by index (optional)
    df = df.sort_index(ascending=False)

    # Convert DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    df.to_csv("output.csv", index=False)

    # Load existing data from SQLite table (if exists)
    try:
        existing_df = pd.read_sql(f"SELECT * FROM {collection_name}", con)
    except Exception as e:
        existing_df = (
            pd.DataFrame()
        )  # If table does not exist, create an empty DataFrame

    # Only concatenate if existing_df is not empty
    if not existing_df.empty:
        merged_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        merged_df = df

    # Drop duplicates based on primary key columns (assuming CodDip_Timb and TimeIn_Timb are unique identifiers)
    merged_df.drop_duplicates(
        subset=["CodDip_Timb", "TimeIn_Timb"], keep="last", inplace=True
    )

    # Write merged DataFrame back to the SQLite table
    merged_df.to_sql(collection_name, con, if_exists="replace", index=False)

    # Create a 'month_year' column in string format
    merged_df["month_year"] = merged_df["TimeIn_Timb"].apply(
        lambda x: pd.to_datetime(x).to_period("M").strftime("%Y-%m")
    )

    # Debugging: Print merged DataFrame
    print("Merged DataFrame:")
    print(merged_df.head())

    unique_months = merged_df["month_year"].unique()
    print("Unique months:", unique_months)

    def create_monthly_table(month, df):
        Orari = f"data_{month.replace('-', '_')}"
        df.to_sql(Orari, con, if_exists="replace", index=False)

    for month in unique_months:
        if month != "NaT":
            month_data = merged_df[merged_df["month_year"] == month]
            create_monthly_table(month, month_data)

except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()
