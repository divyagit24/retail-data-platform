"""
validate.py
-----------
Runs data quality checks on Snowflake RAW tables after ingestion.
Checks for nulls, duplicates, row counts, and value ranges.

Usage:
    python validate.py --table orders
    python validate.py --table all
"""

import os
import logging
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/validate.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "RETAIL_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
    "role":      os.getenv("SNOWFLAKE_ROLE",       "SYSADMIN"),
}

# ── Validation rules per table ─────────────────────────────────────────────────
# Each rule: (check_name, SQL query, expected_result, comparison)
# comparison: "eq" = equals, "gt" = greater than, "eq0" = should be zero

VALIDATION_RULES = {
    "RAW_ORDERS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_ORDERS",
         0, "gt"),

        ("no_null_order_id",
         "SELECT COUNT(*) FROM RAW_ORDERS WHERE ORDER_ID IS NULL",
         0, "eq"),

        ("no_null_customer_id",
         "SELECT COUNT(*) FROM RAW_ORDERS WHERE CUSTOMER_ID IS NULL",
         0, "eq"),

        ("no_duplicate_order_ids",
         "SELECT COUNT(*) FROM (SELECT ORDER_ID, COUNT(*) c FROM RAW_ORDERS GROUP BY ORDER_ID HAVING c > 1)",
         0, "eq"),

        ("valid_order_status",
         """SELECT COUNT(*) FROM RAW_ORDERS
            WHERE ORDER_STATUS NOT IN
            ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved')""",
         0, "eq"),
    ],

    "RAW_ORDER_ITEMS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_ORDER_ITEMS",
         0, "gt"),

        ("no_null_order_id",
         "SELECT COUNT(*) FROM RAW_ORDER_ITEMS WHERE ORDER_ID IS NULL",
         0, "eq"),

        ("no_null_product_id",
         "SELECT COUNT(*) FROM RAW_ORDER_ITEMS WHERE PRODUCT_ID IS NULL",
         0, "eq"),

        ("non_negative_price",
         "SELECT COUNT(*) FROM RAW_ORDER_ITEMS WHERE PRICE < 0",
         0, "eq"),

        ("non_negative_freight",
         "SELECT COUNT(*) FROM RAW_ORDER_ITEMS WHERE FREIGHT_VALUE < 0",
         0, "eq"),
    ],

    "RAW_PRODUCTS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_PRODUCTS",
         0, "gt"),

        ("no_null_product_id",
         "SELECT COUNT(*) FROM RAW_PRODUCTS WHERE PRODUCT_ID IS NULL",
         0, "eq"),

        ("no_duplicate_product_ids",
         "SELECT COUNT(*) FROM (SELECT PRODUCT_ID, COUNT(*) c FROM RAW_PRODUCTS GROUP BY PRODUCT_ID HAVING c > 1)",
         0, "eq"),
    ],

    "RAW_CUSTOMERS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_CUSTOMERS",
         0, "gt"),

        ("no_null_customer_id",
         "SELECT COUNT(*) FROM RAW_CUSTOMERS WHERE CUSTOMER_ID IS NULL",
         0, "eq"),

        ("no_duplicate_customer_ids",
         "SELECT COUNT(*) FROM (SELECT CUSTOMER_ID, COUNT(*) c FROM RAW_CUSTOMERS GROUP BY CUSTOMER_ID HAVING c > 1)",
         0, "eq"),
    ],

    "RAW_ORDER_REVIEWS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_ORDER_REVIEWS",
         0, "gt"),

        ("no_null_review_id",
         "SELECT COUNT(*) FROM RAW_ORDER_REVIEWS WHERE REVIEW_ID IS NULL",
         0, "eq"),

        ("valid_review_score",
         "SELECT COUNT(*) FROM RAW_ORDER_REVIEWS WHERE REVIEW_SCORE NOT BETWEEN 1 AND 5",
         0, "eq"),
    ],

    "RAW_SELLERS": [
        ("row_count_gt_zero",
         "SELECT COUNT(*) FROM RAW_SELLERS",
         0, "gt"),

        ("no_null_seller_id",
         "SELECT COUNT(*) FROM RAW_SELLERS WHERE SELLER_ID IS NULL",
         0, "eq"),

        ("no_duplicate_seller_ids",
         "SELECT COUNT(*) FROM (SELECT SELLER_ID, COUNT(*) c FROM RAW_SELLERS GROUP BY SELLER_ID HAVING c > 1)",
         0, "eq"),
    ],
}


# ── Validation runner ──────────────────────────────────────────────────────────
def run_check(cursor, check_name: str, query: str, expected, comparison: str) -> dict:
    """Execute a single validation check and return the result."""
    try:
        cursor.execute(query)
        actual = cursor.fetchone()[0]

        if comparison == "gt":
            passed = actual > expected
        elif comparison == "eq":
            passed = actual == expected
        else:
            passed = actual == expected

        return {
            "check":    check_name,
            "passed":   passed,
            "actual":   actual,
            "expected": f"> {expected}" if comparison == "gt" else f"== {expected}",
        }
    except Exception as e:
        return {
            "check":   check_name,
            "passed":  False,
            "actual":  None,
            "expected": str(expected),
            "error":   str(e),
        }


def validate_table(conn, table_name: str) -> bool:
    """Run all validation rules for a given table. Returns True if all pass."""
    if table_name not in VALIDATION_RULES:
        logger.warning(f"No validation rules defined for {table_name}")
        return True

    rules = VALIDATION_RULES[table_name]
    cursor = conn.cursor()
    results = []

    logger.info(f"Running {len(rules)} checks on {table_name}...")

    for check_name, query, expected, comparison in rules:
        result = run_check(cursor, check_name, query, expected, comparison)
        results.append(result)

    cursor.close()

    # Print results
    all_passed = True
    logger.info(f"\n{'─'*55}")
    logger.info(f"  Validation Results: {table_name}")
    logger.info(f"{'─'*55}")
    for r in results:
        icon = "✅" if r["passed"] else "❌"
        logger.info(f"  {icon}  {r['check']:<35} actual={r['actual']}")
        if not r["passed"]:
            all_passed = False
            logger.error(f"      FAILED — expected {r['expected']}, got {r['actual']}")
    logger.info(f"{'─'*55}")
    logger.info(f"  Result: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    logger.info(f"{'─'*55}\n")

    return all_passed


def validate_all(conn) -> bool:
    """Run validation on all tables. Returns True if all tables pass."""
    overall = True
    for table_name in VALIDATION_RULES:
        passed = validate_table(conn, table_name)
        if not passed:
            overall = False

    logger.info("=" * 55)
    logger.info(f"OVERALL VALIDATION: {'✅ PASSED' if overall else '❌ FAILED'}")
    logger.info("=" * 55)
    return overall


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Retail Data Platform — Validation Script")
    parser.add_argument("--table", default="all", help="Table to validate (or 'all')")
    args = parser.parse_args()

    os.makedirs("logs", exist_ok=True)

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        if args.table == "all":
            passed = validate_all(conn)
        else:
            table_name = args.table.upper()
            passed = validate_table(conn, table_name)
    finally:
        conn.close()

    # Exit with non-zero code if validation fails (important for Airflow)
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
