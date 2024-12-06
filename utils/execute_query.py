from typing import Dict, List, Any
from contextlib import contextmanager
from mysql.connector import connect, Error as MySQLError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from .mongodb_query_generator import MongoDBQueryGenerator

import logging

class QueryExecutor:
    def __init__(self, mysql_config: Dict[str, str], mongodb_url: str, mongodb_name: str):
        """
        Initialize query executor
        
        Args:
            mysql_config: MySQL connection configuration
            mongodb_url: MongoDB connection URL
            mongodb_name: MongoDB database name
        """
        self.mysql_config = mysql_config
        self.mongodb_url = mongodb_url
        self.mongodb_name = mongodb_name
        self.logger = logging.getLogger(__name__)
        self.mongodb_query_generator = MongoDBQueryGenerator()

    @contextmanager
    def _mysql_connection(self):
        """Context manager for MySQL connection"""
        connection = None
        try:
            connection = connect(**self.mysql_config)
            yield connection
        except MySQLError as e:
            self.logger.error(f"MySQL connection error: {str(e)}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    @contextmanager
    def _mongodb_connection(self):
        """Context manager for MongoDB connection"""
        client = None
        try:
            client = MongoClient(self.mongodb_url)
            yield client
        except PyMongoError as e:
            self.logger.error(f"MongoDB connection error: {str(e)}")
            raise
        finally:
            if client:
                client.close()

    def execute_sql_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query statement
            params: Query parameters (optional)
            
        Returns:
            List of query results
            
        Raises:
            MySQLError: When SQL query execution fails
        """
        try:
            with self._mysql_connection() as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(query, params) if params else cursor.execute(query)
                    results = cursor.fetchall()
                    return results
        except MySQLError as e:
            self.logger.error(f"Error executing SQL query: {str(e)}")
            raise

    def execute_mongodb_query(
        self, 
        collection_name: str, 
        query: Dict[str, Any],
        batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        try:
            with self._mongodb_connection() as client:
                db = client[self.mongodb_name]
                collection = db[collection_name]

                if 'aggregate' in query:
                    cursor = collection.aggregate(
                        query['aggregate'],
                        allowDiskUse=True,
                        batchSize=batch_size
                    )
                else:
                    filter_query = query.get('filter', {})
                    projection = query.get('projection')
                    sort = query.get('sort')
                    limit = query.get('limit')
                    
                    cursor = collection.find(
                        filter_query,
                        projection=projection,
                        batch_size=batch_size
                    )
                    
                    if sort:
                        if isinstance(sort, list):
                            cursor = cursor.sort(sort)
                        else:
                            cursor = cursor.sort(list(sort.items()))
                        
                    if limit:
                        cursor = cursor.limit(limit)

                return list(cursor)
            
        except PyMongoError as e:
            self.logger.error(f"Error executing MongoDB query: {str(e)}")
            raise

    def execute_transaction(self, queries: List[str]) -> None:
        """
        Execute MySQL transaction
        
        Args:
            queries: List of SQL queries
            
        Raises:
            MySQLError: When transaction execution fails
        """
        try:
            with self._mysql_connection() as connection:
                with connection.cursor() as cursor:
                    connection.start_transaction()
                    try:
                        for query in queries:
                            cursor.execute(query)
                        connection.commit()
                    except MySQLError:
                        connection.rollback()
                        raise
        except MySQLError as e:
            self.logger.error(f"Error executing transaction: {str(e)}")
            raise