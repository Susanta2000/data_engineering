from pyspark.sql.window import Window
from pyspark.sql.functions import sum, substring, concat, lit, col, rank, when, round
from src.main.write.database_writer import DatabaseWriter


def calculate_total_sales_done_by_sales_team(sales_team_data_mart_df):
    sales_team_data_mart_df.printSchema()
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
    sales_team_data_mart_df = sales_team_data_mart_df.withColumn("total_sales", sum("total_cost").over(window))\
        .withColumn("sales_month", substring("sales_date", 1, 7)).\
        select("store_id", "sales_person_id", concat(col("sales_person_first_name"), lit(" "),
                col("sales_person_last_name")).alias("full_name"), "sales_month", "total_sales").distinct()

    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(col("total_sales").desc())
    sales_team_data_mart_df = sales_team_data_mart_df.withColumn("rank", rank().over(rank_window))\
        .withColumn("incentive", when(col("rank") == 1, col("total_sales")*0.01).otherwise(lit(0)))\
        .withColumn("incentive", round(col("incentive"), 2))\
        .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")

    DatabaseWriter().write_into_database(sales_team_data_mart_df, "sales_team_final")
