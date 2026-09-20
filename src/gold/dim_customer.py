from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable


import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog

SOURCE_TABLE = f"{CATALOG}.silver.silver_users"
TARGET_TABLE = f"{CATALOG}.gold.dim_customer"


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        customer_sk INT,
        customer_key STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        username STRING,
        birth_date STRING,
        address STRING,
        city STRING,
        state STRING,
        postal_code STRING,
        country STRING,
        company_name STRING,
        company_department STRING,
        job_title STRING,
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


def get_last_processed_version(source_table: str, target_table: str) -> int:

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
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .selectExpr("max(version)")
        .collect()[0][0]
    )


dim_customer_row_count = (
    spark.sql(
        f"SELECT count(*) AS c FROM {TARGET_TABLE}"
    ).collect()[0]["c"]
)


if dim_customer_row_count == 0:

    silver_customers_current = spark.table(SOURCE_TABLE)

    window_spec = Window.orderBy("customer_key")

    dim_customer_initial = (
        silver_customers_current
        .select(
            "customer_key",
            "first_name",
            "last_name",
            "email",
            "phone",
            "username",
            "birth_date",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "company_name",
            "company_department",
            "job_title",
            "record_hash"
        )
        .withColumn(
            "customer_sk",
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
            "customer_sk",
            "customer_key",
            "first_name",
            "last_name",
            "email",
            "phone",
            "username",
            "birth_date",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "company_name",
            "company_department",
            "job_title",
            "record_hash",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "dw_created_at",
            "dw_updated_at"
        )
    )

    dim_customer_initial.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)


current_version_before_changes = get_current_table_version(SOURCE_TABLE)

if get_last_processed_version(SOURCE_TABLE, TARGET_TABLE) == -1:

    set_last_processed_version(
        SOURCE_TABLE,
        TARGET_TABLE,
        current_version_before_changes
    )


last_version = get_last_processed_version(
    SOURCE_TABLE,
    TARGET_TABLE
)

new_version = get_current_table_version(SOURCE_TABLE)


if new_version <= last_version:

    silver_changes = spark.sql(
        f"SELECT * FROM {SOURCE_TABLE} WHERE 1=0"
    )

else:

    silver_changes = spark.sql(f"""
        SELECT
            customer_key,
            first_name,
            last_name,
            email,
            phone,
            username,
            birth_date,
            address,
            city,
            state,
            postal_code,
            country,
            company_name,
            company_department,
            job_title,
            record_hash
        FROM table_changes(
            '{SOURCE_TABLE}',
            {last_version + 1},
            {new_version}
        )
        WHERE _change_type IN ('insert', 'update_postimage')
    """)


current_dim_customer = (
    spark.table(TARGET_TABLE)
    .filter(col("is_current") == True)
)


changed_customers = (
    silver_changes.alias("src")
    .join(
        current_dim_customer
        .select(
            "customer_key",
            col("record_hash").alias("existing_hash")
        )
        .alias("dim"),
        on="customer_key",
        how="inner"
    )
    .filter(
        col("record_hash") != col("existing_hash")
    )
    .select("src.*")
)


new_customers = (
    silver_changes.alias("src")
    .join(
        current_dim_customer
        .select("customer_key")
        .alias("dim"),
        on="customer_key",
        how="left_anti"
    )
)


changed_customers = changed_customers.cache()
new_customers = new_customers.cache()


dim_customer_table = DeltaTable.forName(
    spark,
    TARGET_TABLE
)

changed_keys = [
    row["customer_key"]
    for row in changed_customers
    .select("customer_key")
    .collect()
]


if changed_keys:

    changed_keys_sql = ", ".join(
        [f"'{k}'" for k in changed_keys]
    )

    dim_customer_table.update(
        condition=f"""
            customer_key IN ({changed_keys_sql})
            AND is_current = true
        """,
        set={
            "is_current": "false",
            "effective_end_date": "current_date()",
            "dw_updated_at": "current_timestamp()"
        }
    )


rows_to_insert = (
    changed_customers
    .unionByName(new_customers)
)

insert_count = rows_to_insert.count()


if insert_count > 0:

    max_sk = spark.sql(
        f"""
        SELECT COALESCE(MAX(customer_sk), 0) AS max_sk
        FROM {TARGET_TABLE}
        """
    ).collect()[0]["max_sk"]

    window_spec = Window.orderBy("customer_key")

    rows_final = (
        rows_to_insert
        .withColumn(
            "customer_sk",
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
            "customer_sk",
            "customer_key",
            "first_name",
            "last_name",
            "email",
            "phone",
            "username",
            "birth_date",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "company_name",
            "company_department",
            "job_title",
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


if new_version > last_version:

    set_last_processed_version(
        SOURCE_TABLE,
        TARGET_TABLE,
        new_version
    )


# changed_customers.unpersist()
# new_customers.unpersist()