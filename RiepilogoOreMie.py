from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse

try:
    # Define the path to your Access database
    database_path = r"C:\Users\Informatico\OneDrive - RIZZOLO ENERTECH srl\Desktop\Timbrature-240304.accdb"

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
    query = "SELECT CodDip, TimeIn_Timb, TimeOut_Timb, OreLav_timb FROM Timbrature_T WHERE CodDip = :codDip_value"

    # Execute the query with the parameter using SQLAlchemy's text() function
    with engine.connect() as connection:
        result = connection.execute(text(query).bindparams(codDip_value=codDip_value))

        # Fetch all rows from the executed query
        rows = result.fetchall()

        # Get column names from the result set
        columns = result.keys()

    # Convert the result set to a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Export the DataFrame to a CSV file
    df.to_csv("output.csv", index=False)

    print("Data has been exported to 'output.csv'")
except Exception as e:
    print(f"Error: {e}")
