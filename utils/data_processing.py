import json
import os
import dotenv
from mysql import connector
import pandas as pd
from pymongo import MongoClient
from .format import infer_mysql_data_type, batch

dotenv.load_dotenv()

# Set up the MongoDB client
mongodb_url = os.getenv('MONGODB_URL')
mongo_client = MongoClient(mongodb_url)
mongo_db = mongo_client[os.getenv('MONGODB_NAME')]

# MySQL connection parameters from environment variables
db_config = {
    'host': os.getenv('DB_HOST'),         # AWS RDS endpoint
    'user': os.getenv('DB_USER'),         # Database user
    'password': os.getenv('DB_PASSWORD'), # Database password
    'database': os.getenv('DB_NAME')      # Database name
}

def upload_json_to_mongodb(file_path: str, collection_name: str, batch_size: int = 1000) -> None:
    """
    Processes a JSON Lines file and stores its content in a specified MongoDB collection.
    :param file_path: Path to the JSON file.
    :param collection_name: Name of the MongoDB collection to store data in.
    :param batch_size: Number of documents to insert in one batch.
    """
    mongo_collection = mongo_db[collection_name]  # Access or create the collection

    with open(file_path, 'r') as f:
        try:
            # Try loading as JSON array
            data = json.load(f)
        except json.JSONDecodeError:
            # Assume JSON Lines if decoding fails
            f.seek(0)  # Reset file pointer
            data = [json.loads(line) for line in f]
    
    if isinstance(data, list):
        # Batch insertion to avoid exceeding MongoDB size limits
        for batch_data in batch(data, batch_size):
            mongo_collection.insert_many(batch_data)
    else:
        # Insert single document
        mongo_collection.insert_one(data)


def upload_csv_to_mysql(file_path: str, table_name: str = 'uploaded_data') -> None:
    """
    Reads a CSV file and uploads its content to an AWS RDS MySQL database with inferred data types.
    :param file_path: Path to the CSV file.
    :param table_name: Name of the MySQL table to store data.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Establish a MySQL connection
    connection = connector.connect(**db_config)
    cursor = connection.cursor()

    # Infer column data types
    columns_with_types = [
        f"`{col}` {infer_mysql_data_type(df[col])}" for col in df.columns
    ]
    columns_definition = ', '.join(columns_with_types)

    # Create table if it doesn't exist
    table_name = table_name.lower()
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});"
    cursor.execute(create_table_query)

    # Insert data row by row
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    # Commit changes and close connection
    connection.commit()
    cursor.close()
    connection.close()

    print(f"CSV data has been uploaded to the '{table_name}' table in MySQL.")