import numpy as np
import urllib.parse
from loguru import logger
import sys
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session
from psycopg2.extensions import register_adapter, AsIs
from agentchunking.database.definitions import (Base,SQLTable,AnnotationTable)
""" psycopg2 throws datatype error into postgres DB.
Following block of code can solve this issue.
Source: https://stackoverflow.com/a/56390591
"""
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int64, addapt_numpy_int64)

class SQLDatabaseManager:
    """SQL database manager. It will save tracklet informations in SQL DB
    """
    def __init__(self, database_config: dict,create_db :bool=False) -> None:
        """create all the SQL tables with appropriate column names.
        Args:
                database_config (dict): config dictionary containing information about databases
                
        """
        self.create_db = create_db
        self.config = database_config
        self.create_engine(database_config)
        self.declare_tables()
        
    def database_exists(self, user: str, password: str, host: str, port: int, database_name: str) -> bool:
        """
        Check if a PostgreSQL database exists.
        """
        try:
            conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()
            return exists
        except psycopg2.Error as e:
            logger.error("Error checking database existence: {}".format(e))
            sys.exit(-1)

    def drop_database(self, user: str, password: str, host: str, port: int, database_name: str):
        """
        Drop (delete) the PostgreSQL database if it exists.
        """
        try:
            # Connect to the 'postgres' database (default maintenance database)
            conn = psycopg2.connect(
                dbname="postgres",
                user=user,
                password=password,
                host=host,
                port=port
            )
            conn.autocommit = True  # Necessary for DROP DATABASE and connection termination
            cursor = conn.cursor()

            # Terminate all active connections to the target database
            terminate_query = sql.SQL("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                AND pid <> pg_backend_pid();
            """)
            cursor.execute(terminate_query, (database_name,))
            logger.info("Terminated all active connections to the database: {}".format(database_name))

            # Drop the database
            drop_query = sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name))
            cursor.execute(drop_query)
            logger.info("Database {} dropped successfully.".format(database_name))

            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            logger.error("Error dropping database {}: {}".format(database_name, e))
            sys.exit(-1)
        except Exception as e:
            logger.error("An unexpected error occurred while dropping the database: {}".format(e))
            sys.exit(-1)


    def create_database(self, user: str, password: str, host: str, port: int, database_name: str):
        """
        Create a new PostgreSQL database.
        """
        try:
            conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
            logger.info("Database {} created successfully.".format(database_name))

            cursor.close()
            conn.close()
        except psycopg2.Error as e:
            logger.error("Error creating database {}: {}".format(database_name, e))  
            sys.exit(-1) 

    def create_engine(self, database_config) -> None:
        """create SQL engine.

        Args:
            database_config : user, password, localhost, port and DB name of the postgres database
        Returns:
            None
        """
        try:
            conn_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
                database_config['user'],
                database_config['password'],
                database_config['host'],
                database_config['port'],
                database_config['database']
            )

            self.engine = create_engine(conn_url, echo=False)

        except Exception as exc:
            logger.error("Exception occurred while creating SQL engine. Error: {}".format(exc))
            sys.exit(-1)


    def declare_tables(self) -> None:
        """Declare the SQL tables.

        Args:
            self
        Returns:
            None
        """
        try:
            if self.create_db:
                Base.metadata.create_all(self.engine)
            self.annotation_table = SQLTable(self.engine, AnnotationTable.__table__)
        except Exception as exc:
            logger.error('Exception occured while table defining. Error: {}'.format(exc))
            sys.exit(-1)
        
    def annotation_table_insert(self, insert_data: list[dict]) -> int:
        """
        Insert data into the annotation_table.

        Args:
            insert_data (list[dict]): A list of dictionaries, where each dictionary
                                      represents a row to be inserted. Column names
                                      are keys and their values are the data.
        Returns:
            int: Returns 0 if successful, otherwise an error might lead to sys.exit
                 via the underlying SQLTable.insert method.
        """
        logger.info(f"Attempting to insert {len(insert_data)} rows into {self.annotation_table.table.name}.")
        if not hasattr(self, 'annotation_table'):
            logger.error("Annotation table is not initialized in SQLDatabaseManager.")
            sys.exit(-1)

        return_code = self.annotation_table.insert(insert_data)

        if return_code == 0:
            logger.info(f"Successfully inserted data into {self.annotation_table.table.name}.")
        else:
            # This path might not be reached if SQLTable.insert exits on error
            logger.error(f"Failed to insert data into {self.annotation_table.table.name}.")
        return return_code
    

def sql_table_names(engine):
    """get SQL table names from the database
    Args:
        engine: sql engine
    Returns:
        str: returns table names
    """
    insp = inspect(engine)
    table_names = insp.get_table_names()
    logger.info("SQL table names: {}".format(table_names))
    return table_names