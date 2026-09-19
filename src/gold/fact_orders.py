from pyspark.sql.functions import (
    col,
    to_date,
    current_timestamp
)
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc


dbutils.widgets.text("catalog", "miav2databricks")

CATALOG = dbutils.widgets.get("catalog")

SOURCE_TABLE = f"{CATALOG}.silver.silver_carts"
TARGET_TABLE = f"{CATALOG}.gold.fact_orders"

DIM_PRODUCT = f"{CATALOG}.gold.dim_product"
DIM_CUSTOMER = f"{CATALOG}.gold.dim_customer"
DIM_DATE = f"{CATALOG}.gold.dim_date"


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
    cart_id INT,
    product_sk INT,
    customer_sk INT,
    date_key INT,
    product_key STRING,
    customer_key STRING,
    order_date DATE,
    quantity INT,
    unit_price DOUBLE,
    line_total DOUBLE,
    discount_percentage DOUBLE,
    line_discounted_total DOUBLE,
    dw_created_at TIMESTAMP,
    dw_updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


def get_last_processed_version(
    source_table: str,
    target_table: str
) -> int:

    result = spark.sql(f"""
        SELECT last_processed_version
        FROM {CATALOG}.gold._pipeline_state
        WHERE source_table = '{source_table}'
          AND target_table = '{target_table}'
    """).collect()

    return result[0]["last_processed_version"] if result else -1


def set_last_processed_version(
    source_table: str,
    target_table: str,
    version: int
):

    spark.sql(f"""
        MERGE INTO {CATALOG}.gold._pipeline_state AS target

        USING (
            SELECT
                '{source_table}' AS source_table,
                '{target_table}' AS target_table,
                {version} AS last_processed_version,
                current_timestamp() AS updated_at
        ) AS source

        ON target.source_table = source.source_table
        AND target.target_table = source.target_table

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)


def get_current_table_version(table_name: str) -> int:

    return (
        spark.sql(
            f"DESCRIBE HISTORY {table_name}"
        )
        .selectExpr("max(version)")
        .collect()[0][0]
    )


product_id_to_sku = (
    spark.read
    .format("delta")
    .load(
        "abfss://bronze@miav2storage.dfs.core.windows.net/dummyjson/products"
    )
    .select(
        col("id").alias("dummyjson_product_id"),
        col("sku").alias("product_key")
    )
)


last_version = get_last_processed_version(
    SOURCE_TABLE,
    TARGET_TABLE
)

new_version = get_current_table_version(
    SOURCE_TABLE
)


if last_version == -1:

    order_lines_changes = spark.table(
        SOURCE_TABLE
    )

elif new_version <= last_version:

    order_lines_changes = (
        spark.table(SOURCE_TABLE)
        .filter("1=0")
    )

else:

    order_lines_changes = spark.sql(f"""
        SELECT
            cart_id,
            customer_key,
            product_id_ref,
            product_name,
            unit_price,
            quantity,
            line_total,
            discount_percentage,
            line_discounted_total,
            last_updated_ts
        FROM table_changes(
            '{SOURCE_TABLE}',
            {last_version + 1},
            {new_version}
        )
        WHERE _change_type IN ('insert', 'update_postimage')
    """)


dedup_window = (
    Window
    .partitionBy("cart_id", "product_id_ref")
    .orderBy(desc("last_updated_ts"))
)


order_lines_changes = (
    order_lines_changes
    .withColumn(
        "_rn",
        row_number().over(dedup_window)
    )
    .filter(col("_rn") == 1)
    .drop("_rn")
)


order_lines_with_key = (
    order_lines_changes.alias("ol")
    .join(
        product_id_to_sku.alias("lkp"),
        col("ol.product_id_ref")
        == col("lkp.dummyjson_product_id"),
        how="left"
    )
    .withColumn(
        "order_date",
        to_date(col("ol.last_updated_ts"))
    )
    .select(
        col("ol.cart_id"),
        col("ol.customer_key"),
        col("lkp.product_key"),
        col("order_date"),
        col("ol.quantity"),
        col("ol.unit_price"),
        col("ol.line_total"),
        col("ol.discount_percentage"),
        col("ol.line_discounted_total")
    )
)


dim_date_lookup = (
    spark.table(DIM_DATE)
    .select(
        "date_key",
        "full_date"
    )
)


with_date_key = (
    order_lines_with_key.alias("ol")
    .join(
        dim_date_lookup.alias("dd"),
        col("ol.order_date") == col("dd.full_date"),
        how="left"
    )
)


dim_product_lookup = (
    spark.table(DIM_PRODUCT)
    .select(
        col("product_sk"),
        col("product_key").alias("dp_product_key"),
        col("effective_start_date").alias(
            "dp_effective_start_date"
        ),
        col("effective_end_date").alias(
            "dp_effective_end_date"
        )
    )
)


with_product_sk = (
    with_date_key
    .join(
        dim_product_lookup,
        (col("product_key") == col("dp_product_key"))
        & (
            col("order_date")
            >= col("dp_effective_start_date")
        )
        & (
            col("order_date")
            < col("dp_effective_end_date")
        ),
        how="left"
    )
    .drop(
        "dp_product_key",
        "dp_effective_start_date",
        "dp_effective_end_date"
    )
)


dim_customer_lookup = (
    spark.table(DIM_CUSTOMER)
    .select(
        col("customer_sk"),
        col("customer_key").alias("dc_customer_key"),
        col("effective_start_date").alias(
            "dc_effective_start_date"
        ),
        col("effective_end_date").alias(
            "dc_effective_end_date"
        )
    )
)


with_customer_sk = (
    with_product_sk
    .join(
        dim_customer_lookup,
        (col("customer_key") == col("dc_customer_key"))
        & (
            col("order_date")
            >= col("dc_effective_start_date")
        )
        & (
            col("order_date")
            < col("dc_effective_end_date")
        ),
        how="left"
    )
    .drop(
        "dc_customer_key",
        "dc_effective_start_date",
        "dc_effective_end_date"
    )
)


fact_incremental = (
    with_customer_sk
    .select(
        "cart_id",
        "product_sk",
        "customer_sk",
        "date_key",
        "product_key",
        "customer_key",
        "order_date",
        "quantity",
        "unit_price",
        "line_total",
        "discount_percentage",
        "line_discounted_total"
    )
    .withColumn(
        "dw_created_at",
        current_timestamp()
    )
    .withColumn(
        "dw_updated_at",
        current_timestamp()
    )
)


final_dedup_window = (
    Window
    .partitionBy("cart_id", "product_key")
    .orderBy(
        desc("customer_sk"),
        desc("product_sk")
    )
)


fact_incremental = (
    fact_incremental
    .withColumn(
        "_rn",
        row_number().over(final_dedup_window)
    )
    .filter(col("_rn") == 1)
    .drop("_rn")
)


fact_incremental.createOrReplaceTempView(
    "fact_orders_updates"
)


spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS target

    USING fact_orders_updates AS source

    ON target.cart_id = source.cart_id
    AND target.product_key = source.product_key

    WHEN MATCHED THEN UPDATE SET *

    WHEN NOT MATCHED THEN INSERT *
""")


if new_version > last_version:

    set_last_processed_version(
        SOURCE_TABLE,
        TARGET_TABLE,
        new_version
    )