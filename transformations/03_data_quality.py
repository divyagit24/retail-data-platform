# Databricks notebook source
# 03_data_quality.py
# ------------------
# Runs post-transformation data quality checks on ANALYTICS tables.
# Verifies row counts, nulls, and business logic sanity.
# Exits with error if any critical check fails — Airflow will catch this.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

spark = SparkSession.builder.appName("RetailPlatform_DataQuality").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

snowflake_options = {
    "sfURL":       os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser":      os.getenv("SNOWFLAKE_USER"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":  "RETAIL_DB",
    "sfWarehouse": "RETAIL_WH",
    "sfRole":      "SYSADMIN",
}

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def read_analytics(table_name: str):
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**snowflake_options)
        .option("sfSchema", "ANALYTICS")
        .option("dbtable", table_name)
        .load()
    )


# ── Check runner ──────────────────────────────────────────────────────────────
results = []

def check(name: str, passed: bool, detail: str = ""):
    icon = "✅" if passed else "❌"
    results.append({"name": name, "passed": passed, "detail": detail})
    print(f"  {icon}  {name:<45} {detail}")
    return passed


print("\n" + "="*60)
print("POST-TRANSFORM DATA QUALITY CHECKS")
print("="*60)

# ── DAILY_REVENUE checks ───────────────────────────────────────────────────────
print("\n📋 DAILY_REVENUE")
dr = read_analytics("DAILY_REVENUE")
dr_count = dr.count()

check("daily_revenue_has_rows",         dr_count > 0,           f"rows={dr_count:,}")
check("daily_revenue_no_null_dates",    dr.filter(F.col("ORDER_DATE").isNull()).count() == 0)
check("daily_revenue_no_negative",      dr.filter(F.col("TOTAL_REVENUE") < 0).count() == 0)
check("daily_revenue_orders_positive",  dr.filter(F.col("TOTAL_ORDERS") <= 0).count() == 0)

avg_rev = dr.agg(F.avg("TOTAL_REVENUE")).collect()[0][0]
check("daily_revenue_avg_reasonable",   avg_rev and avg_rev > 0, f"avg_daily=${avg_rev:,.2f}" if avg_rev else "")


# ── TOP_PRODUCTS checks ────────────────────────────────────────────────────────
print("\n📋 TOP_PRODUCTS")
tp = read_analytics("TOP_PRODUCTS")
tp_count = tp.count()

check("top_products_has_rows",          tp_count > 0,              f"rows={tp_count:,}")
check("top_products_no_null_id",        tp.filter(F.col("PRODUCT_ID").isNull()).count() == 0)
check("top_products_no_negative_rev",   tp.filter(F.col("TOTAL_REVENUE") < 0).count() == 0)
check("top_products_valid_scores",
      tp.filter((F.col("AVG_REVIEW_SCORE") < 1) |
                (F.col("AVG_REVIEW_SCORE") > 5)).count() == 0)


# ── CUSTOMER_RETENTION checks ──────────────────────────────────────────────────
print("\n📋 CUSTOMER_RETENTION")
cr = read_analytics("CUSTOMER_RETENTION")
cr_count = cr.count()

check("customer_retention_has_rows",    cr_count > 0,             f"rows={cr_count:,}")
check("retention_rate_between_0_100",
      cr.filter((F.col("RETENTION_RATE_PCT") < 0) |
                (F.col("RETENTION_RATE_PCT") > 100)).count() == 0)
check("returning_lte_total",
      cr.filter(F.col("RETURNING_CUSTOMERS") > F.col("TOTAL_CUSTOMERS")).count() == 0)


# ── SELLER_PERFORMANCE checks ─────────────────────────────────────────────────
print("\n📋 SELLER_PERFORMANCE")
sp = read_analytics("SELLER_PERFORMANCE")
sp_count = sp.count()

check("seller_performance_has_rows",    sp_count > 0,             f"rows={sp_count:,}")
check("seller_no_null_id",              sp.filter(F.col("SELLER_ID").isNull()).count() == 0)
check("seller_no_negative_revenue",     sp.filter(F.col("TOTAL_REVENUE") < 0).count() == 0)
check("seller_valid_review_scores",
      sp.filter((F.col("AVG_REVIEW_SCORE") < 1) |
                (F.col("AVG_REVIEW_SCORE") > 5)).count() == 0)
check("seller_late_rate_valid",
      sp.filter((F.col("LATE_DELIVERY_RATE_PCT") < 0) |
                (F.col("LATE_DELIVERY_RATE_PCT") > 100)).count() == 0)


# ── CATEGORY_REVENUE checks ────────────────────────────────────────────────────
print("\n📋 CATEGORY_REVENUE")
catrev = read_analytics("CATEGORY_REVENUE")
check("category_revenue_has_rows",      catrev.count() > 0)
check("category_no_null_category",      catrev.filter(F.col("CATEGORY").isNull()).count() == 0)
check("category_no_negative_revenue",   catrev.filter(F.col("TOTAL_REVENUE") < 0).count() == 0)


# ── ORDER_FULFILLMENT checks ───────────────────────────────────────────────────
print("\n📋 ORDER_FULFILLMENT")
of = read_analytics("ORDER_FULFILLMENT")
check("fulfillment_has_rows",           of.count() > 0)
check("fulfillment_positive_avg_days",  of.filter(F.col("AVG_DELIVERY_DAYS") < 0).count() == 0)
check("fulfillment_late_rate_valid",
      of.filter((F.col("LATE_RATE_PCT") < 0) |
                (F.col("LATE_RATE_PCT") > 100)).count() == 0)


# ── Final summary ──────────────────────────────────────────────────────────────
passed = [r for r in results if r["passed"]]
failed = [r for r in results if not r["passed"]]

print("\n" + "="*60)
print(f"DATA QUALITY SUMMARY: {len(passed)}/{len(results)} checks passed")
print("="*60)

if failed:
    print("\n❌ FAILED CHECKS:")
    for r in failed:
        print(f"   - {r['name']}: {r['detail']}")
    print("\nPipeline halted. Fix data issues before proceeding.")
    sys.exit(1)
else:
    print("\n✅ ALL CHECKS PASSED. Analytics data is clean and ready.")
    print("="*60)
