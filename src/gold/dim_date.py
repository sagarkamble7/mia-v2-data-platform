from pyspark.sql.functions import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()
CATALOG = args.catalog

TARGET_TABLE = f"{CATALOG}.gold.dim_date"


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    date_key INT,
    full_date DATE,
    day_of_week INT,
    day_name STRING,
    day_of_month INT,
    month_number INT,
    month_name STRING,
    quarter_number INT,
    year_number INT
)
USING DELTA
""")


date_df = (
    spark.sql("""
        SELECT explode(
            sequence(
                to_date('2020-01-01'),
                to_date('2030-12-31'),
                interval 1 day
            )
        ) AS full_date
    """)
)


dim_date = (
    date_df
    .withColumn(
        "date_key",
        date_format(col("full_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "day_of_week",
        dayofweek(col("full_date"))
    )
    .withColumn(
        "day_name",
        date_format(col("full_date"), "EEEE")
    )
    .withColumn(
        "day_of_month",
        dayofmonth(col("full_date"))
    )
    .withColumn(
        "month_number",
        month(col("full_date"))
    )
    .withColumn(
        "month_name",
        date_format(col("full_date"), "MMMM")
    )
    .withColumn(
        "quarter_number",
        quarter(col("full_date"))
    )
    .withColumn(
        "year_number",
        year(col("full_date"))
    )
)


dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(TARGET_TABLE)