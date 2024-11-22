import os
import dotenv
import mysql.connector
from pymongo import MongoClient

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

def test_database_connections():
    """Test both database connections and print results."""
    # Test MySQL
    try:
        mysql_tables = get_mysql_tables()
        print(f"MySQL Connection Successful. Tables found: {list(mysql_tables.keys())}")
    except Exception as e:
        print(f"MySQL Connection Error: {str(e)}")

    # Test MongoDB
    try:
        mongo_collections = get_mongodb_collections()
        print(f"MongoDB Connection Successful. Collections found: {list(mongo_collections.keys())}")
    except Exception as e:
        print(f"MongoDB Connection Error: {str(e)}")

# Function to get MongoDB collections
def get_mongodb_collections():
    collections = mongo_db.list_collection_names()
    mongo_details = {}

    def infer_types(value):
        """
        Recursively infer the types of nested structures within dictionaries and lists.
        """
        if isinstance(value, dict):
            # If the value is a dictionary, return a dictionary with inferred types for each key
            return {key: infer_types(val) for key, val in value.items()}
        elif isinstance(value, list):
            # If the value is a list, infer types for each item and collect them in a list
            return [infer_types(item) for item in value]
        else:
            # For non-iterable values, return the type name
            return type(value).__name__

    for collection in collections:
        sample_doc = mongo_db[collection].find_one()
        if sample_doc:
            # Infer types from the sample document
            mongo_details[collection] = {key: infer_types(value) for key, value in sample_doc.items()}
        else:
            mongo_details[collection] = {}

    return mongo_details

# Function to get MySQL tables
def get_mysql_tables():
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s", (db_config['database'],))
    result = cursor.fetchall()
    
    # Organize results by table
    mysql_details = {}
    for table_name, column_name, data_type in result:
        if table_name not in mysql_details:
            mysql_details[table_name] = []
        mysql_details[table_name].append((column_name, data_type))
    
    cursor.close()
    connection.close()
    return mysql_details


def format_nested_fields(fields, indent=2):
    """
    Recursively formats nested fields for display, handling dictionaries and lists.
    :param fields: The fields to format (can be a dict, list, or primitive type).
    :param indent: The indentation level for nested structures.
    :return: A formatted string representation of the fields.
    """
    formatted = ""
    spacer = " " * indent  # Indentation for nested fields

    if isinstance(fields, dict):
        for key, value in fields.items():
            formatted += f"{spacer}• {key}:"
            if isinstance(value, (dict, list)):
                formatted += "\n" + format_nested_fields(value, indent + 2)
            else:
                formatted += f" {value}\n"
    elif isinstance(fields, list):
        for item in fields:
            formatted += f"{spacer}- {format_nested_fields(item, indent + 2)}\n"
    else:
        formatted += f"{fields}"

    return formatted
