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
# MOCKAROO RAW PATHS
# ============================================================

landing_path = "abfss://raw@miav2storage.dfs.core.windows.net/mockaroo"


# ============================================================
# FIX: Mockaroo is a RANDOM DATA GENERATOR, not a stable catalog API.
# Every call to its live endpoint returns a fresh 1000 randomly-generated
# rows with brand-new SKUs -- it does not return "the same 1000 products"
# on a second call. Calling it on every scheduled run means every run
# looks like 1000 genuinely new products to the SCD2 pipeline downstream,
# which is correct SCD2 behavior given the input -- the input itself was
# the problem.
#
# Mockaroo here represents your seed/static product catalog (paired with
# a Type-2 dimension, which implies a catalog that occasionally changes,
# not one that gets fully re-randomized daily). So: pull it exactly once.
# If raw data already exists at landing_path, skip the pull entirely.
# To intentionally reseed (e.g. testing), manually delete landing_path
# first -- this guard does not run automatically, on purpose.
# ============================================================

existing_mockaroo_files = []
try:
    existing_mockaroo_files = dbutils.fs.ls(landing_path)
except Exception:
    existing_mockaroo_files = []

if len(existing_mockaroo_files) > 0:
    print(f"Mockaroo raw data already exists at {landing_path} "
          f"({len(existing_mockaroo_files)} file(s)) -- skipping pull. "
          f"This is expected on every run after the first.")
else:
    print("No existing Mockaroo raw data found -- performing one-time seed pull.")

    url = f"https://api.mockaroo.com/api/{MOCKAROO_SCHEMA_ID}"

    params = {
        "count": MOCKAROO_ROW_COUNT,
        "key": MOCKAROO_API_KEY
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    mockaroo_raw = response.json()
    print(f"Mockaroo records pulled: {len(mockaroo_raw)}")

    for record in mockaroo_raw:
        if record.get("price ") is not None:
            record["price "] = float(record["price "])

    mockaroo_raw_df = spark.createDataFrame(mockaroo_raw)

    dbutils.fs.mkdirs(landing_path)

    (
        mockaroo_raw_df
        .coalesce(1)
        .write
        .mode("append")
        .json(landing_path)
    )

    print("Mockaroo data written to RAW (one-time seed).")


# ============================================================
# DUMMYJSON CONFIGURATION
# (unchanged -- this one is correctly idempotent already: DummyJSON
# returns the SAME 194 products every call, so the hash-gated Silver
# MERGE correctly detects "no change" on repeat runs. No fix needed here.)
# ============================================================

DUMMY_JSON_BASE = "https://dummyjson.com"
DUMMY_JSON_PAGE_LIMIT = 100


def pull_all_dummyjson(endpoint: str, data_key: str):
    all_records = []
    skip = 0

    while True:
        url = f"{DUMMY_JSON_BASE}/{endpoint}"
        params = {"limit": DUMMY_JSON_PAGE_LIMIT, "skip": skip}

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        batch = payload.get(data_key, [])
        all_records.extend(batch)

        total = payload.get("total", len(all_records))
        skip = skip + DUMMY_JSON_PAGE_LIMIT

        if skip >= total:
            break

    return all_records


dj_products_raw = pull_all_dummyjson("products", "products")
print(f"Products pulled: {len(dj_products_raw)}")

dj_user_raw = pull_all_dummyjson("users", "users")
print(f"Users pulled: {len(dj_user_raw)}")

dj_carts_raw = pull_all_dummyjson("carts", "carts")
print(f"Carts pulled: {len(dj_carts_raw)}")


raw_base_path = "abfss://raw@miav2storage.dfs.core.windows.net/dummyjson"
products_path = f"{raw_base_path}/products"
users_path = f"{raw_base_path}/users"
carts_path = f"{raw_base_path}/carts"


def write_raw_json(records, target_path):
    json_rows = [(json.dumps(record),) for record in records]
    (
        spark.createDataFrame(json_rows, ["value"])
        .coalesce(1)
        .write
        .mode("append")
        .text(target_path)
    )


write_raw_json(dj_products_raw, products_path)
write_raw_json(dj_user_raw, users_path)
write_raw_json(dj_carts_raw, carts_path)

print("DummyJSON products written to RAW.")
print("DummyJSON users written to RAW.")
print("DummyJSON carts written to RAW.")
print("RAW extraction completed successfully.")