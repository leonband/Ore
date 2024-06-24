from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse
from pymongo import MongoClient
import json


collection_name = "Orari"

try:
    # MongoDB Connection
    mongo_uri = r"mongodb+srv://leonbanddb:P1rl3tt4@oredilavoro.o3feq5c.mongodb.net/?retryWrites=true&w=majority&appName=OreDiLavoro"
    client = MongoClient(mongo_uri)
    db = client["OreDiLavoro"]
    collection = db[collection_name]

    # Insert a sample document into the collection (optional)
    collection.insert_one({"ciao": "ciao"})
    print("Document inserted into MongoDB successfully!")

except Exception as e:
    print(f"MongoDB Error: {e}")

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
    query = "SELECT TimeIn_Timb, TimeOut_Timb, OreLav_Timb FROM Timbrature_T WHERE CodDip_Timb = 1008"

    # Execute the query with the parameter using SQLAlchemy's text() function
    with engine.connect() as connection:
        result = connection.execute(text(query))

        # Fetch all rows from the executed query
        rows = result.fetchall()

        # Get column names from the result set
        columns = result.keys()

    # Convert the result set to a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Sort DataFrame by index (optional)
    df = df.sort_index(ascending=False)

    # Convert DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    # Insert only new data into MongoDB
    for record in data:
        # Check if the record already exists in MongoDB
        if not collection.find_one(record):
            collection.insert_one(record)
            print(f"Inserted record: {record}")
        else:
            print(f"Record already exists: {record}")

    print("Data upload completed successfully!")
except Exception as e:
    print(f"Error: {e}")
