import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog


failures = []


for dim, key in [
    ("dim_product", "product_key"),
    ("dim_customer", "customer_key")
]:

    dupes = spark.sql(f"""
        SELECT {key}, COUNT(*) AS c
        FROM {CATALOG}.gold.{dim}
        WHERE is_current = true
        GROUP BY {key}
        HAVING c > 1
    """).count()

    if dupes > 0:
        failures.append(
            f"{dim}: {dupes} keys have more than one is_current=true row"
        )

    print(
        f"[{dim}] duplicate-current check: "
        f"{'FAIL' if dupes else 'PASS'}"
    )


unmatched = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {CATALOG}.gold.fact_orders
    WHERE product_sk IS NULL
       OR customer_sk IS NULL
       OR date_key IS NULL
""").collect()[0]["c"]


if unmatched > 0:
    failures.append(
        f"fact_orders: {unmatched} rows have unresolved dimension keys"
    )


print(
    f"[fact_orders] unresolved-key check: "
    f"{'FAIL' if unmatched else 'PASS'}"
)


if failures:
    raise Exception(
        "Gold DQ check FAILED:\n" + "\n".join(failures)
    )


print("All Gold DQ checks passed.")