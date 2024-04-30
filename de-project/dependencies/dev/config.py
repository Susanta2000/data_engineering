""" Development configurations """

# Encryption parameters
KEY = "data-engineering"
IV = "data-engineering-crypto"
SALT = "data-engineering-AesEncryption"

# AWS credentials
AWS_ACCESS_KEY = ""
AWS_SECRET_ACCESS_KEY = ""
REGION = ""

# Database credentials
DB_HOST = "localhost"
DB_USERNAME = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "data_engineering"

# Database properties for pyspark
URL = "jdbc:postgresql://localhost:5432/data_engineering"
PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Database table names
PRODUCT_TABLE = "product"
CUSTOMER_TABLE = "customer"
PRODUCT_STAGING_TABLE = "product_staging_table"
STORE_TABLE = "store"
SALES_TEAM_TABLE = "sales_team"

# S3 local file path
S3_FILE_LOCATION = "/home/cbnits-87/project_data/data_engineering/s3_files"

# S3 downloaded file location
S3_DOWNLOADED_FILE_LOCATION = "/home/cbnits-87/project_data/data_engineering/s3_downloaded_files"

# Bucket name from where we fetch files and process
BUCKET_NAME = "susant-39393-22"
# Folder name
BUCKET_PREFIX = "sales_data/"

# Processed bucket prefix
PROCESSED_BUCKET_PREFIX = "processed_data/"

# Data mart bucket name
DATA_MART_BUCKET_PREFIX = "data_mart/"
# Error folder name
BUCKET_PREFIX_FOR_ERROR_FILES = "sales_error_data/"

# File's schema
FILE_SCHEMA = ["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price",
               "quantity", "total_cost"]

# Local path for data mart
CUSTOMER_DATA_MART_LOCATION = "/home/cbnits-87/project_data/data_engineering/data_mart/customer_data_mart/"
SALES_TEAM_DATA_MART_LOCATION = "/home/cbnits-87/project_data/data_engineering/data_mart/sales_team_data_mart/"
SALES_TEAM_DATA_MART_PARTITION_BY_LOCATION = "/home/cbnits-87/project_data/data_engineering/data_mart/sales_team_partition_by_data_mart"
