import psycopg2
from dependencies.dev.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USERNAME
from loguru import logger
from datetime import datetime, timezone


class DBSession:
    def __init__(self):
        self.connection = None

    def get_db_connection(self) -> object | None:
        """
        Set up connection with database and return connection object.
        :return: Connection object.
        """
        try:
            connection = psycopg2.connect(
                database=DB_NAME,
                host=DB_HOST,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                port=5432
            )
            self.connection = connection
            logger.info("***********Database connection successful***************")
            return connection
        except psycopg2.OperationalError as op_err:
            logger.error(f"Database connection error. Error is: {op_err}")

    def connection_close(self) -> None:
        """
        Close existing database connection.
        :return: None.
        """
        if self.connection:
            self.connection.close()
            logger.info("**************Database connection closed successfully************")
        else:
            logger.warning("Could not close database connection. No database connection is alive.")
