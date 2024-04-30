import findspark
from pyspark.sql import SparkSession
from loguru import logger


findspark.init()


def create_spark_session():
    try:
        spark = SparkSession.builder.master("local[*]")\
                            .appName("de_spark_session")\
                            .config("spark.jars", "/home/cbnits-87/postgresql-42.7.3.jar")\
                            .getOrCreate()
        logger.info("Spark session created----")
        logger.info(spark)
        # spark.stop()
        return spark

    except Exception as error:
        logger.error(f"Error is: {error}")
