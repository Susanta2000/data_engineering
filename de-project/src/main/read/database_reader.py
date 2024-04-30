from loguru import logger


class DatabaseReader:
    def __init__(self, url: str, properties: dict):
        self.url = url
        self.properties = properties

    def create_dataframe(self, spark, table_name: str):
        """
        Create spark DataFrame from table data.
        :param spark: Spark session object.
        :param table_name: PostgreSQL table name.
        :return: Table data as spark DataFrame or None.
        """
        df = None
        try:
            df = spark.read.jdbc(
                url=self.url,
                table=table_name,
                properties=self.properties
            )
        except Exception as err:
            logger.error(f"Error when reading table {table_name} from postgresql. Error is: {err}")
        finally:
            return df
