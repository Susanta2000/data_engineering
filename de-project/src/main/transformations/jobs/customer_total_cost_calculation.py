from pyspark.sql.window import Window
from pyspark.sql.functions import sum
from src.main.write.database_writer import DatabaseWriter


def calculate_total_cost_for_each_customer(customer_data_mart_df):
    window = Window.partitionBy("customer_id", "sales_date")
    customer_data_mart_df = customer_data_mart_df.withColumn("sum", sum("total_cost").over(window)).distinct()

    DatabaseWriter().write_into_database(customer_data_mart_df, "customer_final")
