from pyspark.sql.functions import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog

bronze_users_path = (
    "abfss://bronze@miav2storage.dfs.core.windows.net/dummyjson/users"
)

silver_checkpoint_path = (
    "abfss://silver@miav2storage.dfs.core.windows.net/_checkpoints/silver_users_merge_v1"
)

silver_table = f"{CATALOG}.silver.silver_users"

spark.sql("CREATE SCHEMA IF NOT EXISTS miav2databricks.silver")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        customer_key        STRING,
        first_name          STRING,
        last_name           STRING,
        email               STRING,
        phone               STRING,
        username            STRING,
        birth_date          STRING,
        address             STRING,
        city                STRING,
        state               STRING,
        postal_code         STRING,
        country             STRING,
        company_name        STRING,
        company_department  STRING,
        job_title           STRING,
        record_hash         STRING,
        last_updated_ts     TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


users_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_users_path)
    .select(
        col("id").cast("string").alias("customer_key"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("email"),
        col("phone"),
        col("username"),
        col("birthDate").alias("birth_date"),
        col("address.address").alias("address"),
        col("address.city").alias("city"),
        col("address.state").alias("state"),
        col("address.postalCode").alias("postal_code"),
        col("address.country").alias("country"),
        col("company.name").alias("company_name"),
        col("company.department").alias("company_department"),
        col("company.title").alias("job_title")
    )
)


def merge_silver_users(microbatch_df, batch_id):

    active_spark = microbatch_df.sparkSession

    updates_df = (
        microbatch_df
        .dropDuplicates(["customer_key"])
        .withColumn(
            "record_hash",
            md5(
                concat_ws(
                    "||intervalled||",
                    col("first_name"),
                    col("last_name"),
                    col("email"),
                    col("phone"),
                    col("address"),
                    col("city"),
                    col("state"),
                    col("postal_code"),
                    col("country"),
                    col("company_name"),
                    col("company_department"),
                    col("job_title")
                )
            )
        )
        .withColumn("last_updated_ts", current_timestamp())
    )

    updates_df.createOrReplaceTempView("silver_users_updates")

    active_spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING silver_users_updates AS source

        ON target.customer_key = source.customer_key

        WHEN MATCHED
          AND NOT (target.record_hash <=> source.record_hash)
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
    """)


query = (
    users_stream
    .writeStream
    .foreachBatch(merge_silver_users)
    .outputMode("update")
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()