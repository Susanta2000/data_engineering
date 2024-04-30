import sys
from src.utility.db_session import DBSession
from loguru import logger
from dependencies.dev import config
import os
import traceback
from datetime import datetime
from src.main.read.s3_read import S3Reader
from src.main.download.s3_files_download import S3Downloader
from src.utility.s3_client_session import S3Client
from src.utility.spark_session import create_spark_session
from src.main.copy.copy_invalid_files import CopyInvalidFiles
from src.main.read.database_reader import DatabaseReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import concat_ws, lit, col, expr
from src.main.transformations.jobs.dimension_tables_join import dimension_table_join
from src.main.transformations.jobs.customer_total_cost_calculation import calculate_total_cost_for_each_customer
from src.main.transformations.jobs.sales_team_total_sales_calculation import calculate_total_sales_done_by_sales_team
from src.main.write.file_writer import FileWriter
from src.main.upload.s3_upload import S3Uploader
from src.main.move.move_files_s3_to_s3 import MoveFiles
from src.main.delete.delete_local_files import delete_files_from_local


db_session = DBSession()
connection = db_session.get_db_connection()
# Creating cursor
cursor = connection.cursor()

# Fetch files from S3 and download into local directory
s3_client = S3Client(config.AWS_ACCESS_KEY, config.AWS_SECRET_ACCESS_KEY, config.REGION).get_client()
s3_reader = S3Reader(s3_client, config.BUCKET_NAME, config.BUCKET_PREFIX)
list_of_files = s3_reader.read_files()

if not list_of_files:
    logger.warning("No files found in S3. Exiting...")
    sys.exit(0)

# Check whether in DB we have any file having 'I' status
files = []
for file in list_of_files:
    if file.endswith(".csv"):
        filename = os.path.basename(file)
        files.append(filename)

if not files:
    logger.warning("No CSV files present in S3. Exiting...")
    del files
    sys.exit(0)

query = f"""
    SELECT * FROM product_staging_table
    WHERE file_name in {tuple(files[0:])} AND status='I';
"""
cursor.execute(query)
records = cursor.fetchall()

if records:
    logger.warning("Previous process was incomplete. Exiting...")
    del files
    cursor.close()
    db_session.connection_close()
    sys.exit(0)

cursor.close()
db_session.connection_close()

# Downloading S3 files into local
S3Downloader().download_files(
    s3_client=s3_client,
    bucket=config.BUCKET_NAME,
    local_path=config.S3_DOWNLOADED_FILE_LOCATION,
    s3_files=list_of_files
)

# Creating S3 client
s3_client = S3Client(config.AWS_ACCESS_KEY, config.AWS_SECRET_ACCESS_KEY, config.REGION).get_client()
s3_downloaded_files = config.S3_DOWNLOADED_FILE_LOCATION

csv_files, invalid_files = [], []

for file in os.listdir(s3_downloaded_files):
    filename = os.path.basename(file)  # Getting filename from full path
    if filename.endswith(".csv"):
        csv_files.append(f'{config.S3_DOWNLOADED_FILE_LOCATION}/{file}')
    else:
        invalid_files.append(file)
logger.info("CSV files are: ", csv_files)

# Insert csv files into db
if csv_files:
    for csv_file in csv_files:
        statement = f"""
            INSERT INTO product_staging_table(file_name, file_location, status)
            VALUES ('{csv_file}', '{csv_file}', 'A');
        """
        cursor.execute(statement)
        connection.commit()

########################## SPARK SESSION CREATION ############################
logger.info("*************Creating spark session**************")
spark = create_spark_session()
logger.info("*************Successfully created spark session******************")

# Creating empty DataFrame
# This is the fact DF
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_columns", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema=schema)

#################### SCHEMA VALIDATION #######################
# If a file has same number of columns as in FILE_SCHEMA variable, then this file's schema are
# considered as valid otherwise considered as invalid.
try:
    if csv_files:
        for csv_file in csv_files:
            data_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(csv_file)
            data_columns = data_df.columns
            print("File name--", csv_file)
            extra_columns = list(set(data_columns) - set(config.FILE_SCHEMA))
            if extra_columns:
                logger.info(f"Extra columns in file {csv_file} is/are {extra_columns}")
                data_df = data_df.withColumn("additional_columns", concat_ws(", ", *extra_columns)).select(
                    "customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_columns")
                logger.info("Successfully added extra columns into DataFrame")
            else:
                if len(data_columns) < len(config.FILE_SCHEMA):
                    invalid_files.append(csv_file)
                    continue

                data_df = data_df.withColumn("additional_columns", lit(None)).select(
                    "customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_columns"
                )
            print("Columns---", data_df.columns)
            final_df_to_process = final_df_to_process.union(data_df)
    else:
        logger.error("No CSV file(s) found. Exiting...")
        # raise FileNotFoundError("No CSV file(s) found to process.")
        sys.exit(0)
except Exception as err:
    logger.error(f"Error is: {err}")
    print(traceback.format_exc())

final_df_to_process.show()

# Move error files (invalid files) into another bucket or folder in S3
if invalid_files:
    logger.info("Invalid file(s) found. Moving these files...")
    CopyInvalidFiles().copy_invalid_files(
        s3_client,
        config.BUCKET_NAME,
        config.BUCKET_PREFIX,
        config.BUCKET_PREFIX_FOR_ERROR_FILES,
        invalid_files
    )

db_reader = DatabaseReader(config.URL, config.PROPERTIES)

# Create DataFrame for customer table
logger.info("********Creating Customer table DataFrame**********")
customer_table_df = db_reader.create_dataframe(spark, config.CUSTOMER_TABLE)
customer_table_df.show()

# Create DataFrame for product table
logger.info("********Creating Product table DataFrame**********")
product_table_df = db_reader.create_dataframe(spark, config.PRODUCT_TABLE)
product_table_df.show()

# Create DataFrame for sales team table
logger.info("********Creating Sales team table DataFrame**********")
sales_team_table_df = db_reader.create_dataframe(spark, config.SALES_TEAM_TABLE)
sales_team_table_df.show()

# Create DataFrame for store table
logger.info("********Creating Store table DataFrame**********")
store_table_df = db_reader.create_dataframe(spark, config.STORE_TABLE)
store_table_df.show()

# Create DataFrame for prodict staging table
logger.info("********Creating Product staging table DataFrame**********")
product_staging_table_df = db_reader.create_dataframe(spark, config.PRODUCT_STAGING_TABLE)
product_staging_table_df.show()

s3_customer_store_sales_df_join = dimension_table_join(final_df_to_process, customer_table_df,
            sales_team_table_df, store_table_df)

dataframe_writer = FileWriter("overwrite", "parquet")

# Write customer data into parquet file. First write and save file in local then move into S3
logger.info("Writing customer data into customer data mart...")
customer_data_mart_df = s3_customer_store_sales_df_join.select("customer_id", "first_name", "last_name", "customer_address",
        "pincode","phone_number", "sales_date", "total_cost")
dataframe_writer.write_dataframe(customer_data_mart_df, config.CUSTOMER_DATA_MART_LOCATION)
logger.info(f"Customer data write into {config.CUSTOMER_DATA_MART_LOCATION} location")

s3_uploader = S3Uploader(s3_client)

logger.info("Uploading customer data into s3 datamart...")
s3_uploader.upload_to_s3(
    config.CUSTOMER_DATA_MART_LOCATION,
    config.BUCKET_NAME,
    config.DATA_MART_BUCKET_PREFIX
)
logger.info("Successfully uploaded customer data into s3 datamart")

logger.info("**********Writing sales team data mart into local***************")
sales_team_data_mart_df = s3_customer_store_sales_df_join.select("store_id", "sales_person_id",
        "sales_person_first_name", "sales_person_last_name", "store_manager_name", "manager_id", "is_manager",
        "sales_person_address", "sales_person_pincode", "sales_date", "total_cost",
        expr("SUBSTRING(sales_date, 1, 7) as sales_month"))
dataframe_writer.write_dataframe(sales_team_data_mart_df, config.SALES_TEAM_DATA_MART_LOCATION)
logger.info(f"Sales team data write into {config.SALES_TEAM_DATA_MART_LOCATION} location")

logger.info("Uploading sales team data into s3 datamart...")
s3_uploader.upload_to_s3(
    config.SALES_TEAM_DATA_MART_LOCATION,
    config.BUCKET_NAME,
    config.DATA_MART_BUCKET_PREFIX
)
logger.info("Successfully uploaded sales team data into s3 datamart")

# Writing sales team data by partitioning
logger.info("*************** Writing sales team data in a partition way ***************")
sales_team_data_mart_df.write.format("parquet"). \
    option("header", "true"). \
    mode("overwrite"). \
    partitionBy("sales_month", "store_id"). \
    option("path", config.SALES_TEAM_DATA_MART_PARTITION_BY_LOCATION). \
    save()

# Calculate total cost in a month for each customer
logger.info("******* Calculating total cost in every month for each customer *********")
# calculate_total_cost_for_each_customer(customer_data_mart_df)

# Calculate total cost in a month for each sales person
logger.info("******* Calculating total cost in every month for each sales person *********")
calculate_total_sales_done_by_sales_team(sales_team_data_mart_df)

# Move files to a processed folder
MoveFiles(s3_client).move_processed_files(
    bucket_name=config.BUCKET_NAME,
    source_prefix=config.BUCKET_PREFIX,
    destination_prefix=config.PROCESSED_BUCKET_PREFIX
)

# Delete customer data mart from local
delete_files_from_local(config.CUSTOMER_DATA_MART_LOCATION)

# Delete sales team data mart from local
delete_files_from_local(config.SALES_TEAM_DATA_MART_LOCATION)

# Delete sales team data mart partition from local
delete_files_from_local(config.SALES_TEAM_DATA_MART_PARTITION_BY_LOCATION)

# Update files status
if len(csv_files) == 1:
    statement = f"""
        UPDATE product_staging_table
        SET status='I' AND updated_date=CURRENT_TIMESTAMP
        WHERE file_name in ('{csv_files[0]}');
    """
else:
    statement = f"""
        UPDATE product_staging_table
        SET status='I' AND updated_date=CURRENT_TIMESTAMP
        WHERE file_name in {tuple(csv_files)};
    """
cursor.execute(statement)
connection.commit()

spark.stop()

input("Press Enter to Terminate")
