from typing import Dict, List, Any, Union
from contextlib import contextmanager
from mysql.connector import connect, Error as MySQLError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging

class QueryExecutor:
    def __init__(self, mysql_config: Dict[str, str], mongodb_url: str, mongodb_name: str):
        """
        初始化查詢執行器
        
        Args:
            mysql_config: MySQL 連接配置
            mongodb_url: MongoDB 連接 URL
            mongodb_name: MongoDB 數據庫名稱
        """
        self.mysql_config = mysql_config
        self.mongodb_url = mongodb_url
        self.mongodb_name = mongodb_name
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def _mysql_connection(self):
        """MySQL 連接的上下文管理器"""
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
        """MongoDB 連接的上下文管理器"""
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
        執行 SQL 查詢並返回結果
        
        Args:
            query: SQL 查詢語句
            params: 查詢參數（可選）
            
        Returns:
            查詢結果列表
            
        Raises:
            MySQLError: 當 SQL 查詢執行出錯時
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
        pipeline: Union[Dict[str, Any], List[Dict[str, Any]]],
        batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        執行 MongoDB 查詢並返回結果
        
        Args:
            collection_name: 集合名稱
            pipeline: 查詢或聚合管道
            batch_size: 批次大小
            
        Returns:
            查詢結果列表
            
        Raises:
            PyMongoError: 當 MongoDB 查詢執行出錯時
        """
        try:
            with self._mongodb_connection() as client:
                db = client[self.mongodb_name]
                collection = db[collection_name]

                if isinstance(pipeline, list):
                    cursor = collection.aggregate(
                        pipeline,
                        allowDiskUse=True,
                        batchSize=batch_size
                    )
                else:
                    cursor = collection.find(
                        pipeline.get('find', {}),
                        batch_size=batch_size
                    )

                return list(cursor)
        except PyMongoError as e:
            self.logger.error(f"Error executing MongoDB query: {str(e)}")
            raise

    def execute_transaction(self, queries: List[str]) -> None:
        """
        執行 MySQL 事務
        
        Args:
            queries: SQL 查詢列表
            
        Raises:
            MySQLError: 當事務執行出錯時
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
