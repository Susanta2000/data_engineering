## Project Title
### Incentive Distribution to sales persons in a sales organization

## Project Description
A sales organization wants to give incentive to their sales persons according their sold. They give incentive those
sales persons who have sold maximum on every month in a year.

## Project Scope
Data Engineering

## Tools and Technologies Used
PyCharm IDE, Python3.10, PySpark, AWS S3, PostgreSQL.

## How to run
Step-1: You should have any IDE and python3.10+.<br>
Step-2: Apache spark should also be installed.<br>
Step-3: Create a virtual environment.<br>
Step-4: Go to dependencies ---> dev directory and install all required libraries by running below command:<br>
```pip install -r dev-dependencies.txt```<br>
Step-5: Then set AWS credentials (access key and secret key) in config file.
Step-6: When all gets installed run main.py file inside src/main/transformations/jobs/ directory.

## Project Structure

```plaintext
de-project
├── dependencies
│   ├── dev
│   │   ├── config.py
│   │   ├── dev-dependencies.txt
│   └── prod
│       ├── config.py
│       └── prod-dependencies.txt
├── src
│   ├── __init__.py
│   ├── main
│   │   ├── copy
│   │   │   ├── copy_invalid_files.py
│   │   ├── delete
│   │   │   └── delete_local_files.py
│   │   ├── download
│   │   │   └── s3_files_download.py
│   │   ├── __init__.py
│   │   ├── move
│   │   │   └── move_files_s3_to_s3.py
│   │   ├── read
│   │   │   ├── database_reader.py
│   │   │   └── s3_read.py
│   │   ├── transformations
│   │   │   ├── __init__.py
│   │   │   └── jobs
│   │   │       ├── customer_total_cost_calculation.py
│   │   │       ├── dimension_tables_join.py
│   │   │       ├── __init__.py
│   │   │       ├── main.py
│   │   │       └── sales_team_total_sales_calculation.py
│   │   ├── upload
│   │   │   └── s3_upload.py
│   │   └── write
│   │       ├── database_writer.py
│   │       ├── file_writer.py
│   ├── sql_scripts
│   │   └── scripts.sql
│   └── utility
│       ├── db_session.py
│       ├── encrypt_decrypt.py
│       ├── __init__.py
│       ├── s3_client_session.py
│       └── spark_session.py
└── test
    ├── dev_tests
    │   ├── generate_csv_data.py
    │   ├── generate_csv_data_with_extra_columns.py
    │   ├── generate_csv_data_with_less_columns.py
    └── prod_tests
        └── test.py
```
