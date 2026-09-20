from pyspark.sql.functions import *
from pyspark.sql.types import *


# ============================================================
# MOCKAROO READ AND WRITE AUTOLOADER
# ============================================================

schema_path = "abfss://bronze@miav2storage.dfs.core.windows.net/_schemas/mockaroo_products"

mockaroo_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .load("abfss://raw@miav2storage.dfs.core.windows.net/mockaroo")
)

mockaroo_bronze_df = mockaroo_df.select(
    col("product_id ").cast("long").alias("product_id"),
    col("product_name ").cast("string").alias("product_name"),
    col("category ").cast("string").alias("category"),
    col("price ").cast("double").alias("price"),
    col("brand").cast("string").alias("brand"),
    col("stock_quantity ").cast("long").alias("stock_quantity"),
    col("sku ").cast("string").alias("sku"),
    col("created_at ").cast("string").alias("created_at"),
)

query = (
    mockaroo_bronze_df.writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        "abfss://bronze@miav2storage.dfs.core.windows.net/_checkpoints/mockaroo_products"
    )
    .outputMode("append")
    .trigger(availableNow=True)
    .start("abfss://bronze@miav2storage.dfs.core.windows.net/mockaroo_products")
)


# ============================================================
# DUMMY JSON DATA READING AND WRITING
# ============================================================

from pyspark.sql.functions import input_file_name, current_timestamp

raw_base_path = "abfss://raw@miav2storage.dfs.core.windows.net/dummyjson"

schema_base_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/_schemas/dummyjson"
)

bronze_base_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/dummyjson"
)

bronze_checkpoint_base = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/_checkpoints/dummyjson"
)


def stream_dummyjson_entity_to_bronze(entity_name: str):

    source_path = f"{raw_base_path}/{entity_name}"
    schema_path = f"{schema_base_path}/{entity_name}"
    checkpoint_path = f"{bronze_checkpoint_base}/{entity_name}"
    target_path = f"{bronze_base_path}/{entity_name}"

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )

    bronze_stream = raw_stream.select(
    "*",
    col("_metadata.file_path").alias("_source_file"),
    current_timestamp().alias("_ingested_at")
    )

    query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(target_path)
    )

    query.awaitTermination()

    print(f"[{entity_name}] Bronze files written to: {target_path}")


stream_dummyjson_entity_to_bronze("products")
stream_dummyjson_entity_to_bronze("users")
stream_dummyjson_entity_to_bronze("carts")