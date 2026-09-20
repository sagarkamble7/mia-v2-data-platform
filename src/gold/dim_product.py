from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog

SOURCE_TABLE = f"{CATALOG}.silver.silver_products"
TARGET_TABLE = f"{CATALOG}.gold.dim_product"


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold._pipeline_state (
    source_table STRING,
    target_table STRING,
    last_processed_version BIGINT,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    product_sk INT,
    product_key STRING,
    product_name STRING,
    category STRING,
    price DOUBLE,
    brand STRING,
    stock_quantity INT,
    source_system STRING,
    record_hash STRING,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    dw_created_at TIMESTAMP,
    dw_updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


def get_last_processed_version():
    result = spark.sql(f"""
        SELECT last_processed_version
        FROM {CATALOG}.gold._pipeline_state
        WHERE source_table = '{SOURCE_TABLE}'
          AND target_table = '{TARGET_TABLE}'
    """).collect()

    if result:
        return result[0]["last_processed_version"]

    return None


def set_last_processed_version(version):
    spark.sql(f"""
        MERGE INTO {CATALOG}.gold._pipeline_state AS target
        USING (
            SELECT
                '{SOURCE_TABLE}' AS source_table,
                '{TARGET_TABLE}' AS target_table,
                {version} AS last_processed_version,
                current_timestamp() AS updated_at
        ) AS source
        ON target.source_table = source.source_table
        AND target.target_table = source.target_table

        WHEN MATCHED THEN UPDATE SET
            target.last_processed_version = source.last_processed_version,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT *
    """)


def get_current_table_version(table_name):
    result = spark.sql(
        f"DESCRIBE HISTORY {table_name} LIMIT 1"
    ).collect()

    return result[0]["version"]


dim_product_row_count = spark.sql(
    f"SELECT count(*) AS c FROM {TARGET_TABLE}"
).collect()[0]["c"]


if dim_product_row_count == 0:

    silver_products_current = spark.table(SOURCE_TABLE)

    window_spec = Window.orderBy("product_key")

    dim_product_initial = (
        silver_products_current
        .select(
            "product_key",
            "product_name",
            "category",
            "price",
            "brand",
            "stock_quantity",
            "source_system",
            "record_hash"
        )
        .withColumn(
            "product_sk",
            row_number().over(window_spec)
        )
        .withColumn(
            "effective_start_date",
            lit("1900-01-01").cast("date")
        )
        .withColumn(
            "effective_end_date",
            lit("9999-12-31").cast("date")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
        .withColumn(
            "dw_created_at",
            current_timestamp()
        )
        .withColumn(
            "dw_updated_at",
            current_timestamp()
        )
        .select(
            "product_sk",
            "product_key",
            "product_name",
            "category",
            "price",
            "brand",
            "stock_quantity",
            "source_system",
            "record_hash",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "dw_created_at",
            "dw_updated_at"
        )
    )

    dim_product_initial.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)


current_version_before_changes = get_current_table_version(SOURCE_TABLE)

if get_last_processed_version() is None:
    set_last_processed_version(current_version_before_changes)


last_version = get_last_processed_version()
new_silver_version = get_current_table_version(SOURCE_TABLE)


if new_silver_version <= last_version:

    silver_changes = spark.createDataFrame(
        [],
        spark.table(SOURCE_TABLE).schema
    )

else:

    silver_changes = spark.sql(f"""
        SELECT
            product_key,
            product_name,
            category,
            price,
            brand,
            stock_quantity,
            source_system,
            record_hash
        FROM table_changes(
            '{SOURCE_TABLE}',
            {last_version + 1},
            {new_silver_version}
        )
        WHERE _change_type IN ('insert', 'update_postimage')
    """)


current_dim_product = (
    spark.table(TARGET_TABLE)
    .filter(col("is_current") == True)
)


changed_products = (
    silver_changes.alias("src")
    .join(
        current_dim_product
        .select(
            "source_system",
            "product_key",
            col("record_hash").alias("existing_hash")
        )
        .alias("dim"),
        on=["source_system", "product_key"],
        how="inner"
    )
    .filter(
        col("record_hash") != col("existing_hash")
    )
    .select("src.*")
)


new_products = (
    silver_changes.alias("src")
    .join(
        current_dim_product
        .select(
            "source_system",
            "product_key"
        )
        .alias("dim"),
        on=["source_system", "product_key"],
        how="left_anti"
    )
)


changed_products = changed_products.cache()
new_products = new_products.cache()


dim_product_table = DeltaTable.forName(
    spark,
    TARGET_TABLE
)

changed_products_keys = (
    changed_products
    .select("source_system", "product_key")
    .collect()
)


if changed_products_keys:

    for row in changed_products_keys:

        dim_product_table.update(
            condition=f"""
                source_system = '{row['source_system']}'
                AND product_key = '{row['product_key']}'
                AND is_current = true
            """,
            set={
                "is_current": "false",
                "effective_end_date": "current_date()",
                "dw_updated_at": "current_timestamp()"
            }
        )


rows_to_insert = changed_products.unionByName(
    new_products
)


if rows_to_insert.count() > 0:

    max_sk = spark.sql(
        f"""
        SELECT COALESCE(MAX(product_sk), 0) AS max_sk
        FROM {TARGET_TABLE}
        """
    ).collect()[0]["max_sk"]

    window_spec = Window.orderBy("product_key")

    rows_final = (
        rows_to_insert
        .withColumn(
            "product_sk",
            row_number().over(window_spec) + lit(max_sk)
        )
        .withColumn(
            "effective_start_date",
            current_date()
        )
        .withColumn(
            "effective_end_date",
            lit("9999-12-31").cast("date")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
        .withColumn(
            "dw_created_at",
            current_timestamp()
        )
        .withColumn(
            "dw_updated_at",
            current_timestamp()
        )
        .select(
            "product_sk",
            "product_key",
            "product_name",
            "category",
            "price",
            "brand",
            "stock_quantity",
            "source_system",
            "record_hash",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "dw_created_at",
            "dw_updated_at"
        )
    )

    rows_final.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)


set_last_processed_version(new_silver_version)

# changed_products.unpersist()
# new_products.unpersist()