-- =============================================================================
-- setup.sql
-- Snowflake setup for Retail Data Platform
-- Run this in your Snowflake worksheet before starting the pipeline
-- =============================================================================


-- =============================================================================
-- 1. WAREHOUSE
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 120          -- suspend after 2 mins idle (saves credits)
    AUTO_RESUME    = TRUE
    COMMENT        = 'Retail Data Platform warehouse';


-- =============================================================================
-- 2. DATABASE & SCHEMAS
-- =============================================================================
CREATE DATABASE IF NOT EXISTS RETAIL_DB
    COMMENT = 'Retail Data Platform — Olist E-Commerce';

USE DATABASE RETAIL_DB;

-- Three-layer medallion architecture
CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Raw ingested data — never modified';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Cleaned and conformed data';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Business-ready aggregated metrics';


-- =============================================================================
-- 3. RAW TABLES
-- =============================================================================
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_ORDERS (
    ORDER_ID                      VARCHAR(50)   NOT NULL,
    CUSTOMER_ID                   VARCHAR(50)   NOT NULL,
    ORDER_STATUS                  VARCHAR(20),
    ORDER_PURCHASE_TIMESTAMP      TIMESTAMP_NTZ,
    ORDER_APPROVED_AT             TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_DATE  TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
    _INGESTED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE                  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW_ORDER_ITEMS (
    ORDER_ID            VARCHAR(50)  NOT NULL,
    ORDER_ITEM_ID       NUMBER(10,0) NOT NULL,
    PRODUCT_ID          VARCHAR(50)  NOT NULL,
    SELLER_ID           VARCHAR(50),
    SHIPPING_LIMIT_DATE TIMESTAMP_NTZ,
    PRICE               NUMBER(10,2),
    FREIGHT_VALUE       NUMBER(10,2),
    _INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW_PRODUCTS (
    PRODUCT_ID                 VARCHAR(50) NOT NULL,
    PRODUCT_CATEGORY_NAME      VARCHAR(100),
    PRODUCT_NAME_LENGTH        NUMBER(10,0),
    PRODUCT_DESCRIPTION_LENGTH NUMBER(10,0),
    PRODUCT_PHOTOS_QTY         NUMBER(10,0),
    PRODUCT_WEIGHT_G           NUMBER(10,2),
    PRODUCT_LENGTH_CM          NUMBER(10,2),
    PRODUCT_HEIGHT_CM          NUMBER(10,2),
    PRODUCT_WIDTH_CM           NUMBER(10,2),
    _INGESTED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE               VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    CUSTOMER_ID              VARCHAR(50) NOT NULL,
    CUSTOMER_UNIQUE_ID       VARCHAR(50),
    CUSTOMER_ZIP_CODE_PREFIX VARCHAR(10),
    CUSTOMER_CITY            VARCHAR(100),
    CUSTOMER_STATE           VARCHAR(10),
    _INGESTED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE             VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW_ORDER_REVIEWS (
    REVIEW_ID               VARCHAR(50) NOT NULL,
    ORDER_ID                VARCHAR(50) NOT NULL,
    REVIEW_SCORE            NUMBER(2,0),
    REVIEW_COMMENT_TITLE    VARCHAR(500),
    REVIEW_COMMENT_MESSAGE  VARCHAR(5000),
    REVIEW_CREATION_DATE    TIMESTAMP_NTZ,
    REVIEW_ANSWER_TIMESTAMP TIMESTAMP_NTZ,
    _INGESTED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE            VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW_SELLERS (
    SELLER_ID              VARCHAR(50) NOT NULL,
    SELLER_ZIP_CODE_PREFIX VARCHAR(10),
    SELLER_CITY            VARCHAR(100),
    SELLER_STATE           VARCHAR(10),
    _INGESTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE           VARCHAR(100)
);

-- Audit log table for pipeline runs
CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT_LOG (
    LOG_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME    VARCHAR(100),
    ROWS_LOADED   NUMBER(15,0),
    STATUS        VARCHAR(20),
    ERROR_MESSAGE VARCHAR(2000),
    RUN_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- 4. STAGING TABLES
-- =============================================================================
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS STG_ORDERS (
    ORDER_ID                      VARCHAR(50)   NOT NULL PRIMARY KEY,
    CUSTOMER_ID                   VARCHAR(50)   NOT NULL,
    ORDER_STATUS                  VARCHAR(20),
    ORDER_PURCHASE_DATE           DATE,
    ORDER_PURCHASE_TIMESTAMP      TIMESTAMP_NTZ,
    ORDER_APPROVED_AT             TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_DATE  TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
    DAYS_TO_DELIVERY              NUMBER(6,2),
    IS_LATE_DELIVERY              BOOLEAN,
    _CREATED_AT                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_ORDER_ITEMS (
    ORDER_ID       VARCHAR(50)  NOT NULL,
    ORDER_ITEM_ID  NUMBER(10,0) NOT NULL,
    PRODUCT_ID     VARCHAR(50)  NOT NULL,
    SELLER_ID      VARCHAR(50),
    PRICE          NUMBER(10,2),
    FREIGHT_VALUE  NUMBER(10,2),
    TOTAL_VALUE    NUMBER(10,2),
    _CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID)
);


-- =============================================================================
-- 5. ANALYTICS TABLES
-- =============================================================================
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS DAILY_REVENUE (
    ORDER_DATE      DATE         NOT NULL PRIMARY KEY,
    TOTAL_ORDERS    NUMBER(10,0),
    TOTAL_REVENUE   NUMBER(15,2),
    AVG_ORDER_VALUE NUMBER(10,2),
    _UPDATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS TOP_PRODUCTS (
    PRODUCT_ID          VARCHAR(50)  NOT NULL PRIMARY KEY,
    PRODUCT_CATEGORY    VARCHAR(100),
    TOTAL_UNITS_SOLD    NUMBER(10,0),
    TOTAL_REVENUE       NUMBER(15,2),
    AVG_REVIEW_SCORE    NUMBER(4,2),
    _UPDATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CUSTOMER_RETENTION (
    REPORT_MONTH           VARCHAR(7)   NOT NULL PRIMARY KEY,  -- YYYY-MM
    TOTAL_CUSTOMERS        NUMBER(10,0),
    RETURNING_CUSTOMERS    NUMBER(10,0),
    RETENTION_RATE_PCT     NUMBER(5,2),
    _UPDATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SELLER_PERFORMANCE (
    SELLER_ID           VARCHAR(50)  NOT NULL PRIMARY KEY,
    SELLER_STATE        VARCHAR(10),
    TOTAL_ORDERS        NUMBER(10,0),
    TOTAL_REVENUE       NUMBER(15,2),
    AVG_REVIEW_SCORE    NUMBER(4,2),
    AVG_DELIVERY_DAYS   NUMBER(6,2),
    _UPDATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- 6. VERIFY SETUP
-- =============================================================================
SHOW SCHEMAS IN DATABASE RETAIL_DB;
SHOW TABLES IN SCHEMA RETAIL_DB.RAW;
SHOW TABLES IN SCHEMA RETAIL_DB.STAGING;
SHOW TABLES IN SCHEMA RETAIL_DB.ANALYTICS;

-- Expected output: 3 schemas, 7 RAW tables, 2 STAGING tables, 4 ANALYTICS tables
