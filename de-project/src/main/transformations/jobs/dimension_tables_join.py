from loguru import logger
from pyspark.sql.functions import col


def dimension_table_join(final_df_to_process, customer_table_df, sales_team_table_df, store_table_df):
    logger.info("Joining final_df_to_process to customer_table_df")
    final_df_join_customer_df = final_df_to_process.alias("s3_data").join(
        customer_table_df.alias("ct"),
        col("s3_data.customer_id") == col("ct.customer_id"), "inner"
    ).withColumn("customer_address", col("ct.address")).drop("product_name", "price", "quantity", "additional_columns",
              col("s3_data.customer_id"), col("ct.address"), "customer_joining_date")

    customer_df_join_store_df = final_df_join_customer_df.join(
        store_table_df,
        final_df_join_customer_df["store_id"] == store_table_df["id"],
        "inner"
    ).withColumn("store_address", col("address")).drop("id", col("address"), "store_pincode", "store_opening_date", "reviews")

    s3_customer_store_sales_df_join = customer_df_join_store_df.alias("cs").join(
        sales_team_table_df.alias("st"),
        sales_team_table_df["id"] == customer_df_join_store_df["sales_person_id"],
        "inner"
    ).withColumn("sales_person_first_name", col("st.first_name")).withColumn(
        "sales_person_last_name", col("st.last_name")).withColumn(
        "sales_person_address", col("st.address")).withColumn(
        "sales_person_pincode", col("st.pincode")).drop("id", col("st.first_name"), col("st.last_name"),
            col("st.address"), col("st.pincode"))

    return s3_customer_store_sales_df_join
