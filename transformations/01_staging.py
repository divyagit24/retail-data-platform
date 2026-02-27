# Databricks notebook source
# 01_staging.py
# -------------
# Transforms RAW schema tables into clean, conformed STAGING tables.
# Run this notebook in Databricks Community Edition.
#
# Steps:
#   1. Connect to Snowflake
#   2. Read RAW tables into Spark DataFrames
#   3. Apply cleaning & transformation logic
#   4. Write to STAGING schema in Snowflake

# ── 1. Install Snowflake Spark connector (run once) ───────────────────────────
# %pip install snowflake-spark-connector

# ── 2. Imports ────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
import os

spark = SparkSession.builder.appName("RetailPlatform_Staging").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Spark session started.")
print(f"Spark version: {spark.version}")

# ── 3. Snowflake connection options ───────────────────────────────────────────
# In Databricks: store credentials in Databricks Secrets or as env vars
snowflake_options = {
    "sfURL":       os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser":      os.getenv("SNOWFLAKE_USER"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":  "RETAIL_DB",
    "sfWarehouse": "RETAIL_WH",
    "sfRole":      "SYSADMIN",
}

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def read_raw(table_name: str):
    """Read a table from Snowflake RAW schema into a Spark DataFrame."""
    print(f"Reading RAW.{table_name}...")
    df = (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**snowflake_options)
        .option("sfSchema", "RAW")
        .option("dbtable", table_name)
        .load()
    )
    print(f"  Loaded {df.count():,} rows from {table_name}")
    return df


def write_staging(df, table_name: str):
    """Write a DataFrame to Snowflake STAGING schema."""
    print(f"Writing {df.count():,} rows to STAGING.{table_name}...")
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**snowflake_options)
        .option("sfSchema", "STAGING")
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )
    print(f"  ✅ Written to STAGING.{table_name}")


# ── 4. Transform: Orders ───────────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming ORDERS")
print("="*60)

raw_orders = read_raw("RAW_ORDERS")

stg_orders = (
    raw_orders

    # Drop audit columns from RAW
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    # Parse timestamps
    .withColumn("ORDER_PURCHASE_TIMESTAMP",
                F.to_timestamp("ORDER_PURCHASE_TIMESTAMP"))
    .withColumn("ORDER_APPROVED_AT",
                F.to_timestamp("ORDER_APPROVED_AT"))
    .withColumn("ORDER_DELIVERED_CARRIER_DATE",
                F.to_timestamp("ORDER_DELIVERED_CARRIER_DATE"))
    .withColumn("ORDER_DELIVERED_CUSTOMER_DATE",
                F.to_timestamp("ORDER_DELIVERED_CUSTOMER_DATE"))
    .withColumn("ORDER_ESTIMATED_DELIVERY_DATE",
                F.to_timestamp("ORDER_ESTIMATED_DELIVERY_DATE"))

    # Extract date from purchase timestamp
    .withColumn("ORDER_PURCHASE_DATE",
                F.to_date("ORDER_PURCHASE_TIMESTAMP"))

    # Compute days to delivery (actual vs estimated)
    .withColumn("DAYS_TO_DELIVERY",
                F.round(
                    F.datediff(
                        F.col("ORDER_DELIVERED_CUSTOMER_DATE"),
                        F.col("ORDER_PURCHASE_TIMESTAMP")
                    ).cast(DoubleType()), 2
                ))

    # Flag late deliveries
    .withColumn("IS_LATE_DELIVERY",
                F.when(
                    F.col("ORDER_DELIVERED_CUSTOMER_DATE") >
                    F.col("ORDER_ESTIMATED_DELIVERY_DATE"),
                    True
                ).otherwise(False))

    # Standardize status to lowercase
    .withColumn("ORDER_STATUS", F.lower(F.trim(F.col("ORDER_STATUS"))))

    # Remove rows with no order_id or customer_id
    .filter(F.col("ORDER_ID").isNotNull() & F.col("CUSTOMER_ID").isNotNull())

    # Deduplicate
    .dropDuplicates(["ORDER_ID"])

    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Orders after transform: {stg_orders.count():,} rows")
stg_orders.printSchema()
write_staging(stg_orders, "STG_ORDERS")


# ── 5. Transform: Order Items ─────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming ORDER ITEMS")
print("="*60)

raw_items = read_raw("RAW_ORDER_ITEMS")

stg_items = (
    raw_items
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    # Cast numeric fields
    .withColumn("PRICE",         F.col("PRICE").cast(DoubleType()))
    .withColumn("FREIGHT_VALUE", F.col("FREIGHT_VALUE").cast(DoubleType()))

    # Compute total value per line item
    .withColumn("TOTAL_VALUE",
                F.round(F.col("PRICE") + F.col("FREIGHT_VALUE"), 2))

    # Remove negatives
    .filter(F.col("PRICE") >= 0)
    .filter(F.col("FREIGHT_VALUE") >= 0)

    # Remove rows missing key fields
    .filter(F.col("ORDER_ID").isNotNull() & F.col("PRODUCT_ID").isNotNull())

    .dropDuplicates(["ORDER_ID", "ORDER_ITEM_ID"])
    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Order items after transform: {stg_items.count():,} rows")
write_staging(stg_items, "STG_ORDER_ITEMS")


# ── 6. Transform: Products ────────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming PRODUCTS")
print("="*60)

raw_products = read_raw("RAW_PRODUCTS")

stg_products = (
    raw_products
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    # Fill unknown category
    .withColumn("PRODUCT_CATEGORY_NAME",
                F.when(F.col("PRODUCT_CATEGORY_NAME").isNull(), "unknown")
                .otherwise(F.lower(F.trim(F.col("PRODUCT_CATEGORY_NAME")))))

    # Replace underscores with spaces in category name for readability
    .withColumn("PRODUCT_CATEGORY_NAME",
                F.regexp_replace("PRODUCT_CATEGORY_NAME", "_", " "))

    .filter(F.col("PRODUCT_ID").isNotNull())
    .dropDuplicates(["PRODUCT_ID"])
    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Products after transform: {stg_products.count():,} rows")
write_staging(stg_products, "STG_PRODUCTS")


# ── 7. Transform: Customers ───────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming CUSTOMERS")
print("="*60)

raw_customers = read_raw("RAW_CUSTOMERS")

stg_customers = (
    raw_customers
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    .withColumn("CUSTOMER_CITY",
                F.initcap(F.lower(F.trim(F.col("CUSTOMER_CITY")))))
    .withColumn("CUSTOMER_STATE",
                F.upper(F.trim(F.col("CUSTOMER_STATE"))))

    .filter(F.col("CUSTOMER_ID").isNotNull())
    .dropDuplicates(["CUSTOMER_ID"])
    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Customers after transform: {stg_customers.count():,} rows")
write_staging(stg_customers, "STG_CUSTOMERS")


# ── 8. Transform: Reviews ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming ORDER REVIEWS")
print("="*60)

raw_reviews = read_raw("RAW_ORDER_REVIEWS")

stg_reviews = (
    raw_reviews
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    .withColumn("REVIEW_SCORE", F.col("REVIEW_SCORE").cast(IntegerType()))

    # Sentiment label based on score
    .withColumn("SENTIMENT",
                F.when(F.col("REVIEW_SCORE") >= 4, "positive")
                .when(F.col("REVIEW_SCORE") == 3, "neutral")
                .otherwise("negative"))

    # Keep only valid scores
    .filter(F.col("REVIEW_SCORE").between(1, 5))
    .filter(F.col("REVIEW_ID").isNotNull())

    .dropDuplicates(["REVIEW_ID"])
    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Reviews after transform: {stg_reviews.count():,} rows")
write_staging(stg_reviews, "STG_REVIEWS")


# ── 9. Transform: Sellers ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("Transforming SELLERS")
print("="*60)

raw_sellers = read_raw("RAW_SELLERS")

stg_sellers = (
    raw_sellers
    .drop("_INGESTED_AT", "_SOURCE_FILE")

    .withColumn("SELLER_CITY",
                F.initcap(F.lower(F.trim(F.col("SELLER_CITY")))))
    .withColumn("SELLER_STATE",
                F.upper(F.trim(F.col("SELLER_STATE"))))

    .filter(F.col("SELLER_ID").isNotNull())
    .dropDuplicates(["SELLER_ID"])
    .withColumn("_CREATED_AT", F.current_timestamp())
)

print(f"Sellers after transform: {stg_sellers.count():,} rows")
write_staging(stg_sellers, "STG_SELLERS")


# ── 10. Summary ───────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGING TRANSFORMATION COMPLETE")
print("="*60)
print("  STG_ORDERS       ✅")
print("  STG_ORDER_ITEMS  ✅")
print("  STG_PRODUCTS     ✅")
print("  STG_CUSTOMERS    ✅")
print("  STG_REVIEWS      ✅")
print("  STG_SELLERS      ✅")
print("="*60)
