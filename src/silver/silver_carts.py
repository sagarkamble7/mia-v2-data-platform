from pyspark.sql.functions import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog


bronze_carts_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/dummyjson/carts"
)

silver_checkpoint_path = (
    "abfss://silver@miav2storage.dfs.core.windows.net/_checkpoints/silver_carts_merge_v1"
)

silver_table = f"{CATALOG}.silver.silver_carts"

spark.sql("CREATE SCHEMA IF NOT EXISTS miav2databricks.silver")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        cart_id INT,
        customer_key STRING,
        product_id_ref INT,
        product_name STRING,
        unit_price DOUBLE,
        quantity INT,
        line_total DOUBLE,
        discount_percentage DOUBLE,
        line_discounted_total DOUBLE,
        record_hash STRING,
        last_updated_ts TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


carts_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_carts_path)
)


silver_carts = (
    carts_stream
    .select(
        col("id").alias("cart_id"),
        col("userId").cast("string").alias("customer_key"),
        explode("products").alias("product")
    )
    .select(
        col("cart_id"),
        col("customer_key"),
        col("product.id").alias("product_id_ref"),
        col("product.title").alias("product_title"),
        col("product.price").alias("unit_price"),
        col("product.quantity").alias("quantity"),
        col("product.total").alias("line_total"),
        col("product.discountPercentage").alias("discount_percentage"),
        col("product.discountedTotal").alias("line_discounted_total")
    )
)


def merge_silver_carts(microbatch_df, batch_id):

    active_spark = microbatch_df.sparkSession

    silver_order_lines_with_hash = (
        microbatch_df
        .dropDuplicates(["cart_id", "product_id_ref"])
        .withColumn(
            "record_hash",
            md5(
                concat_ws(
                    "|",
                    col("cart_id").cast("string"),
                    col("product_id_ref").cast("string"),
                    col("quantity").cast("string"),
                    col("unit_price").cast("string"),
                    col("line_total").cast("string"),
                    col("discount_percentage").cast("string")
                )
            )
        )
        .withColumn("last_updated_ts", current_timestamp())
    )

    silver_order_lines_with_hash.createOrReplaceTempView(
        "silver_order_lines_updates"
    )

    active_spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING silver_order_lines_updates AS source

        ON target.cart_id = source.cart_id
        AND target.product_id_ref = source.product_id_ref

        WHEN MATCHED
          AND NOT (target.record_hash <=> source.record_hash)
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
    """)


query = (
    silver_carts
    .writeStream
    .foreachBatch(merge_silver_carts)
    .outputMode("update")
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()