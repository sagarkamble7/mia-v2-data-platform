from pyspark.sql.functions import *


bronze_mockaroo_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/mockaroo_products"
)

bronze_dummyjson_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/dummyjson/products"
)

silver_checkpoint_path = (
    "abfss://silver@miav2storage.dfs.core.windows.net/_checkpoints/silver_products_merge_v1"
)

silver_table = "miav2databricks.silver.silver_products"


spark.sql("CREATE SCHEMA IF NOT EXISTS miav2databricks.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS miav2databricks.silver.silver_products (
        product_key          STRING,
        product_name         STRING,
        category             STRING,
        price                DECIMAL(12,2),
        brand                STRING,
        stock_quantity       LONG,
        created_at           DATE,
        rating               DOUBLE,
        discount_percentage  DOUBLE,
        dimensions_json      STRING,
        reviews_json         STRING,
        source_system        STRING,
        record_hash          STRING,
        last_updated_ts      TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


mockaroo_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_mockaroo_path)
    .select(
        col("sku").alias("product_key"),
        col("product_name"),
        col("category"),
        col("price").cast("decimal(12,2)"),
        col("brand"),
        col("stock_quantity").cast("long"),
        to_date(col("created_at"), "M/d/yyyy").alias("created_at"),
        lit(None).cast("double").alias("rating"),
        lit(None).cast("double").alias("discount_percentage"),
        lit(None).cast("string").alias("dimensions_json"),
        lit(None).cast("string").alias("reviews_json"),
        lit("mockaroo").alias("source_system")
    )
)


dummyjson_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_dummyjson_path)
    .select(
        col("sku").alias("product_key"),
        col("title").alias("product_name"),
        col("category"),
        col("price").cast("decimal(12,2)"),
        col("brand"),
        col("stock").cast("long").alias("stock_quantity"),
        to_date(
            to_timestamp(
                col("meta.createdAt"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSX"
            )
        ).alias("created_at"),
        col("rating").cast("double"),
        col("discountPercentage").cast("double").alias("discount_percentage"),
        to_json(col("dimensions")).alias("dimensions_json"),
        to_json(col("reviews")).alias("reviews_json"),
        lit("dummyjson").alias("source_system")
    )
)


silver_products_stream = mockaroo_stream.unionByName(dummyjson_stream)


def merge_silver_products(microbatch_df, batch_id):

    active_spark = microbatch_df.sparkSession

    updates_df = (
        microbatch_df
        .dropDuplicates(["source_system", "product_key"])
        .withColumn(
            "record_hash",
            md5(
                concat_ws(
                    "||",
                    col("product_name"),
                    col("category"),
                    col("price").cast("string"),
                    col("brand"),
                    col("stock_quantity").cast("string"),
                    col("created_at").cast("string"),
                    col("rating").cast("string"),
                    col("discount_percentage").cast("string")
                )
            )
        )
        .withColumn("last_updated_ts", current_timestamp())
    )

    updates_df.createOrReplaceTempView("silver_products_updates")

    active_spark.sql("""
        MERGE INTO miav2databricks.silver.silver_products AS target
        USING silver_products_updates AS source

        ON  target.source_system = source.source_system
        AND target.product_key = source.product_key

        WHEN MATCHED
          AND NOT (target.record_hash <=> source.record_hash)
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
    """)


query = (
    silver_products_stream
    .writeStream
    .foreachBatch(merge_silver_products)
    .outputMode("update")
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()