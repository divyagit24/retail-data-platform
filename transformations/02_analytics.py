# Databricks notebook source
# 02_analytics.py
# ---------------
# Computes business metrics from STAGING tables and writes to ANALYTICS schema.
# Produces: daily revenue, top products, customer retention, seller performance,
#           order fulfillment time, and average order value by category.
#
# Run AFTER 01_staging.py has completed successfully.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName("RetailPlatform_Analytics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Snowflake connection ───────────────────────────────────────────────────────
snowflake_options = {
    "sfURL":       os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser":      os.getenv("SNOWFLAKE_USER"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":  "RETAIL_DB",
    "sfWarehouse": "RETAIL_WH",
    "sfRole":      "SYSADMIN",
}

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def read_staging(table_name: str):
    """Read a table from Snowflake STAGING schema."""
    print(f"Reading STAGING.{table_name}...")
    df = (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**snowflake_options)
        .option("sfSchema", "STAGING")
        .option("dbtable", table_name)
        .load()
    )
    print(f"  Loaded {df.count():,} rows")
    return df


def write_analytics(df, table_name: str):
    """Write a DataFrame to Snowflake ANALYTICS schema."""
    count = df.count()
    print(f"Writing {count:,} rows to ANALYTICS.{table_name}...")
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**snowflake_options)
        .option("sfSchema", "ANALYTICS")
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )
    print(f"  ✅ Written to ANALYTICS.{table_name} ({count:,} rows)")
    return count


# ── Load staging tables ────────────────────────────────────────────────────────
orders    = read_staging("STG_ORDERS")
items     = read_staging("STG_ORDER_ITEMS")
products  = read_staging("STG_PRODUCTS")
customers = read_staging("STG_CUSTOMERS")
reviews   = read_staging("STG_REVIEWS")
sellers   = read_staging("STG_SELLERS")

# Register as temp views for SQL-style queries
orders.createOrReplaceTempView("orders")
items.createOrReplaceTempView("items")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")
reviews.createOrReplaceTempView("reviews")
sellers.createOrReplaceTempView("sellers")


# ── Metric 1: Daily Revenue ────────────────────────────────────────────────────
print("\n" + "="*60)
print("Computing: DAILY REVENUE")
print("="*60)

daily_revenue = spark.sql("""
    SELECT
        o.ORDER_PURCHASE_DATE                            AS ORDER_DATE,
        COUNT(DISTINCT o.ORDER_ID)                       AS TOTAL_ORDERS,
        ROUND(SUM(i.TOTAL_VALUE), 2)                     AS TOTAL_REVENUE,
        ROUND(AVG(i.TOTAL_VALUE), 2)                     AS AVG_ORDER_VALUE,
        COUNT(DISTINCT o.CUSTOMER_ID)                    AS UNIQUE_CUSTOMERS,
        ROUND(SUM(i.PRICE), 2)                           AS PRODUCT_REVENUE,
        ROUND(SUM(i.FREIGHT_VALUE), 2)                   AS FREIGHT_REVENUE,
        CURRENT_TIMESTAMP()                              AS _UPDATED_AT
    FROM orders o
    JOIN items  i ON o.ORDER_ID = i.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
      AND o.ORDER_PURCHASE_DATE IS NOT NULL
    GROUP BY o.ORDER_PURCHASE_DATE
    ORDER BY ORDER_DATE
""")

print("Sample daily revenue:")
daily_revenue.orderBy(F.col("ORDER_DATE").desc()).show(5, truncate=False)
write_analytics(daily_revenue, "DAILY_REVENUE")


# ── Metric 2: Top Products ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("Computing: TOP PRODUCTS")
print("="*60)

top_products = spark.sql("""
    SELECT
        i.PRODUCT_ID,
        p.PRODUCT_CATEGORY_NAME                         AS PRODUCT_CATEGORY,
        COUNT(DISTINCT i.ORDER_ID)                      AS TOTAL_ORDERS,
        SUM(i.ORDER_ITEM_ID)                            AS TOTAL_UNITS_SOLD,
        ROUND(SUM(i.PRICE), 2)                          AS TOTAL_REVENUE,
        ROUND(AVG(i.PRICE), 2)                          AS AVG_PRICE,
        ROUND(AVG(r.REVIEW_SCORE), 2)                   AS AVG_REVIEW_SCORE,
        COUNT(r.REVIEW_ID)                              AS REVIEW_COUNT,
        CURRENT_TIMESTAMP()                             AS _UPDATED_AT
    FROM items i
    LEFT JOIN products p ON i.PRODUCT_ID = p.PRODUCT_ID
    LEFT JOIN orders   o ON i.ORDER_ID   = o.ORDER_ID
    LEFT JOIN reviews  r ON o.ORDER_ID   = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY i.PRODUCT_ID, p.PRODUCT_CATEGORY_NAME
    ORDER BY TOTAL_REVENUE DESC
""")

print(f"Total distinct products: {top_products.count():,}")
print("Top 10 products by revenue:")
top_products.show(10, truncate=False)
write_analytics(top_products, "TOP_PRODUCTS")


# ── Metric 3: Customer Retention ──────────────────────────────────────────────
print("\n" + "="*60)
print("Computing: CUSTOMER RETENTION")
print("="*60)

# Count orders per unique customer per month
customer_orders = spark.sql("""
    SELECT
        c.CUSTOMER_UNIQUE_ID,
        DATE_FORMAT(o.ORDER_PURCHASE_TIMESTAMP, 'yyyy-MM') AS ORDER_MONTH,
        COUNT(o.ORDER_ID)                                   AS ORDER_COUNT
    FROM orders   o
    JOIN customers c ON o.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY c.CUSTOMER_UNIQUE_ID, ORDER_MONTH
""")

customer_orders.createOrReplaceTempView("customer_orders")

retention = spark.sql("""
    SELECT
        ORDER_MONTH                                             AS REPORT_MONTH,
        COUNT(DISTINCT CUSTOMER_UNIQUE_ID)                      AS TOTAL_CUSTOMERS,
        COUNT(DISTINCT CASE WHEN ORDER_COUNT > 1
                       THEN CUSTOMER_UNIQUE_ID END)             AS RETURNING_CUSTOMERS,
        ROUND(
            COUNT(DISTINCT CASE WHEN ORDER_COUNT > 1
                           THEN CUSTOMER_UNIQUE_ID END) * 100.0
            / COUNT(DISTINCT CUSTOMER_UNIQUE_ID), 2
        )                                                       AS RETENTION_RATE_PCT,
        CURRENT_TIMESTAMP()                                     AS _UPDATED_AT
    FROM customer_orders
    GROUP BY ORDER_MONTH
    ORDER BY REPORT_MONTH
""")

print("Customer retention by month:")
retention.show(10, truncate=False)
write_analytics(retention, "CUSTOMER_RETENTION")


# ── Metric 4: Seller Performance ──────────────────────────────────────────────
print("\n" + "="*60)
print("Computing: SELLER PERFORMANCE")
print("="*60)

seller_performance = spark.sql("""
    SELECT
        i.SELLER_ID,
        s.SELLER_STATE,
        s.SELLER_CITY,
        COUNT(DISTINCT i.ORDER_ID)                       AS TOTAL_ORDERS,
        SUM(i.ORDER_ITEM_ID)                             AS TOTAL_UNITS_SOLD,
        ROUND(SUM(i.PRICE), 2)                           AS TOTAL_REVENUE,
        ROUND(AVG(i.PRICE), 2)                           AS AVG_ITEM_PRICE,
        ROUND(AVG(r.REVIEW_SCORE), 2)                    AS AVG_REVIEW_SCORE,
        ROUND(AVG(o.DAYS_TO_DELIVERY), 2)                AS AVG_DELIVERY_DAYS,
        SUM(CASE WHEN o.IS_LATE_DELIVERY THEN 1 ELSE 0 END) AS LATE_DELIVERIES,
        ROUND(
            SUM(CASE WHEN o.IS_LATE_DELIVERY THEN 1 ELSE 0 END) * 100.0
            / COUNT(DISTINCT i.ORDER_ID), 2
        )                                                AS LATE_DELIVERY_RATE_PCT,
        CURRENT_TIMESTAMP()                              AS _UPDATED_AT
    FROM items    i
    JOIN orders   o ON i.ORDER_ID   = o.ORDER_ID
    JOIN sellers  s ON i.SELLER_ID  = s.SELLER_ID
    LEFT JOIN reviews r ON o.ORDER_ID = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY i.SELLER_ID, s.SELLER_STATE, s.SELLER_CITY
    ORDER BY TOTAL_REVENUE DESC
""")

print(f"Total sellers analyzed: {seller_performance.count():,}")
print("Top 10 sellers by revenue:")
seller_performance.show(10, truncate=False)
write_analytics(seller_performance, "SELLER_PERFORMANCE")


# ── Metric 5: Category Revenue Summary ────────────────────────────────────────
print("\n" + "="*60)
print("Computing: CATEGORY REVENUE SUMMARY")
print("="*60)

category_revenue = spark.sql("""
    SELECT
        COALESCE(p.PRODUCT_CATEGORY_NAME, 'unknown')    AS CATEGORY,
        COUNT(DISTINCT i.ORDER_ID)                       AS TOTAL_ORDERS,
        SUM(i.ORDER_ITEM_ID)                             AS TOTAL_UNITS_SOLD,
        ROUND(SUM(i.PRICE), 2)                           AS TOTAL_REVENUE,
        ROUND(AVG(i.PRICE), 2)                           AS AVG_PRICE,
        ROUND(AVG(r.REVIEW_SCORE), 2)                    AS AVG_REVIEW_SCORE,
        CURRENT_TIMESTAMP()                              AS _UPDATED_AT
    FROM items   i
    LEFT JOIN products p ON i.PRODUCT_ID = p.PRODUCT_ID
    LEFT JOIN orders   o ON i.ORDER_ID   = o.ORDER_ID
    LEFT JOIN reviews  r ON o.ORDER_ID   = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY p.PRODUCT_CATEGORY_NAME
    ORDER BY TOTAL_REVENUE DESC
""")

print("Revenue by category:")
category_revenue.show(15, truncate=False)
write_analytics(category_revenue, "CATEGORY_REVENUE")


# ── Metric 6: Order Fulfillment Time ──────────────────────────────────────────
print("\n" + "="*60)
print("Computing: ORDER FULFILLMENT TIME")
print("="*60)

fulfillment = spark.sql("""
    SELECT
        DATE_FORMAT(ORDER_PURCHASE_TIMESTAMP, 'yyyy-MM')  AS REPORT_MONTH,
        COUNT(ORDER_ID)                                    AS TOTAL_ORDERS,
        ROUND(AVG(DAYS_TO_DELIVERY), 2)                    AS AVG_DELIVERY_DAYS,
        ROUND(MIN(DAYS_TO_DELIVERY), 2)                    AS MIN_DELIVERY_DAYS,
        ROUND(MAX(DAYS_TO_DELIVERY), 2)                    AS MAX_DELIVERY_DAYS,
        SUM(CASE WHEN IS_LATE_DELIVERY THEN 1 ELSE 0 END) AS LATE_ORDERS,
        ROUND(
            SUM(CASE WHEN IS_LATE_DELIVERY THEN 1 ELSE 0 END) * 100.0
            / COUNT(ORDER_ID), 2
        )                                                  AS LATE_RATE_PCT,
        CURRENT_TIMESTAMP()                                AS _UPDATED_AT
    FROM orders
    WHERE ORDER_STATUS   = 'delivered'
      AND DAYS_TO_DELIVERY IS NOT NULL
      AND DAYS_TO_DELIVERY >= 0
    GROUP BY REPORT_MONTH
    ORDER BY REPORT_MONTH
""")

print("Order fulfillment by month:")
fulfillment.show(10, truncate=False)
write_analytics(fulfillment, "ORDER_FULFILLMENT")


# ── Final Summary ──────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ANALYTICS COMPUTATION COMPLETE")
print("="*60)
print("  DAILY_REVENUE       ✅")
print("  TOP_PRODUCTS        ✅")
print("  CUSTOMER_RETENTION  ✅")
print("  SELLER_PERFORMANCE  ✅")
print("  CATEGORY_REVENUE    ✅")
print("  ORDER_FULFILLMENT   ✅")
print("="*60)
print("\nAll analytics tables are ready in Snowflake ANALYTICS schema.")
print("Connect Tableau or any BI tool to RETAIL_DB.ANALYTICS to visualize.")
