from pyspark.sql.functions import *
import requests
from datetime import datetime, timezone
import json


# ============================================================
# MOCKAROO CONFIGURATION
# ============================================================

MOCKAROO_SCHEMA_ID = "7640ba60"

MOCKAROO_API_KEY = dbutils.secrets.get(
    scope="mia-secrets",
    key="mockaroo-api-key"
)

MOCKAROO_ROW_COUNT = 1000

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


# ============================================================
# READ MOCKAROO DATA
# ============================================================

url = f"https://api.mockaroo.com/api/{MOCKAROO_SCHEMA_ID}"

params = {
    "count": MOCKAROO_ROW_COUNT,
    "key": MOCKAROO_API_KEY
}

response = requests.get(
    url,
    params=params,
    timeout=30
)

response.raise_for_status()

mockaroo_raw = response.json()

print(f"Mockaroo records pulled: {len(mockaroo_raw)}")


# ============================================================
# DUMMYJSON CONFIGURATION
# ============================================================

DUMMY_JSON_BASE = "https://dummyjson.com"

DUMMY_JSON_PAGE_LIMIT = 100


# ============================================================
# DUMMYJSON PAGINATION FUNCTION
# ============================================================

def pull_all_dummyjson(endpoint: str, data_key: str):

    all_records = []

    skip = 0

    while True:

        url = f"{DUMMY_JSON_BASE}/{endpoint}"

        params = {
            "limit": DUMMY_JSON_PAGE_LIMIT,
            "skip": skip
        }

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        payload = response.json()

        batch = payload.get(data_key, [])

        all_records.extend(batch)

        total = payload.get(
            "total",
            len(all_records)
        )

        skip = skip + DUMMY_JSON_PAGE_LIMIT

        if skip >= total:
            break

    return all_records


# ============================================================
# READ DUMMYJSON PRODUCTS
# ============================================================

dj_products_raw = pull_all_dummyjson(
    "products",
    "products"
)

print(
    f"Products pulled: {len(dj_products_raw)}"
)


# ============================================================
# READ DUMMYJSON USERS
# ============================================================

dj_user_raw = pull_all_dummyjson(
    "users",
    "users"
)

print(
    f"Users pulled: {len(dj_user_raw)}"
)


# ============================================================
# READ DUMMYJSON CARTS
# ============================================================

dj_carts_raw = pull_all_dummyjson(
    "carts",
    "carts"
)

print(
    f"Carts pulled: {len(dj_carts_raw)}"
)


# ============================================================
# MOCKAROO RAW PATHS
# ============================================================

landing_path = (
    "abfss://raw@miav2storage.dfs.core.windows.net/"
    "mockaroo"
)

checkpoint_path = (
    "abfss://raw@miav2storage.dfs.core.windows.net/"
    "_checkpoints/mockaroo_products"
)

schema_path = (
    "abfss://raw@miav2storage.dfs.core.windows.net/"
    "_schemas/mockaroo_products"
)

dbutils.fs.ls(
    "abfss://raw@miav2storage.dfs.core.windows.net/"
)

dbutils.fs.mkdirs(landing_path) 
# ============================================================
# WRITE MOCKAROO DATA TO RAW
# ============================================================

for record in mockaroo_raw:

    if record.get("price ") is not None:

        record["price "] = float(
            record["price "]
        )


mockaroo_raw_df = spark.createDataFrame(
    mockaroo_raw
)


(
    mockaroo_raw_df
    .coalesce(1)
    .write
    .mode("append")
    .json(landing_path)
)


print("Mockaroo data written to RAW.")


# ============================================================
# DUMMYJSON RAW PATHS
# ============================================================

raw_base_path = (
    "abfss://raw@miav2storage.dfs.core.windows.net/"
    "dummyjson"
)

products_path = f"{raw_base_path}/products"

users_path = f"{raw_base_path}/users"

carts_path = f"{raw_base_path}/carts"


# ============================================================
# WRITE RAW JSON FUNCTION
# ============================================================

def write_raw_json(records, target_path):

    json_rows = [
        (json.dumps(record),)
        for record in records
    ]

    (
        spark
        .createDataFrame(
            json_rows,
            ["value"]
        )
        .coalesce(1)
        .write
        .mode("append")
        .text(target_path)
    )


# ============================================================
# WRITE DUMMYJSON DATA TO RAW
# ============================================================

write_raw_json(
    dj_products_raw,
    products_path
)

write_raw_json(
    dj_user_raw,
    users_path
)

write_raw_json(
    dj_carts_raw,
    carts_path
)


print("DummyJSON products written to RAW.")
print("DummyJSON users written to RAW.")
print("DummyJSON carts written to RAW.")

print("RAW extraction completed successfully.")