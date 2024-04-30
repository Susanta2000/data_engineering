from dependencies.dev.config import URL, PROPERTIES
from loguru import logger


class DatabaseWriter:
    def __init__(self):
        pass

    def write_into_database(self, df, table_name):
        try:
            df.write.jdbc(
                url=URL,
                table=table_name,
                mode="overwrite",
                properties=PROPERTIES
            )
        except Exception as error:
            logger.error(f"Could not write dataframe into postgresql table. Error is: {error}")
