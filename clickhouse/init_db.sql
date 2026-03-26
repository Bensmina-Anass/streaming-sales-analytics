CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

-- BRONZE LAYER

CREATE TABLE IF NOT EXISTS bronze_geolocation (
    geolocation_zip_code_prefix String,
    geolocation_lat Float64,
    geolocation_lng Float64,
    geolocation_city String,
    geolocation_state String
) ENGINE = MergeTree() ORDER BY geolocation_zip_code_prefix;

CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix String,
    customer_city String,
    customer_state String
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS bronze_sellers (
    seller_id String,
    seller_zip_code_prefix String,
    seller_city String,
    seller_state String
) ENGINE = MergeTree() ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS bronze_products (
    product_id String,
    product_category_name String,
    product_name_length Int32,
    product_description_length Int32,
    product_photos_qty Int32,
    product_weight_g Float64,
    product_length_cm Float64,
    product_height_cm Float64,
    product_width_cm Float64
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS bronze_category_translation (
    product_category_name String,
    product_category_name_english String
) ENGINE = MergeTree() ORDER BY product_category_name;

CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime)
) ENGINE = MergeTree() ORDER BY order_purchase_timestamp;

CREATE TABLE IF NOT EXISTS bronze_order_items (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id String,
    shipping_limit_date DateTime,
    price Float64,
    freight_value Float64
) ENGINE = MergeTree() ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS bronze_order_payments (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Float64
) ENGINE = MergeTree() ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS bronze_order_reviews (
    review_id String,
    order_id String,
    review_score Int32,
    review_comment_title String,
    review_comment_message String,
    review_creation_date DateTime,
    review_answer_timestamp Nullable(DateTime)
) ENGINE = MergeTree() ORDER BY (review_id, order_id);

-- SILVER LAYER

CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix String,
    customer_city String,
    customer_state String,
    customer_lat Float64,
    customer_lng Float64
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS silver_sellers (
    seller_id String,
    seller_zip_code_prefix String,
    seller_city String,
    seller_state String,
    seller_lat Float64,
    seller_lng Float64
) ENGINE = MergeTree() ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS silver_products (
    product_id String,
    product_category_name_english String,
    product_weight_g Float64,
    product_length_cm Float64,
    product_height_cm Float64,
    product_width_cm Float64
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS silver_orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime)
) ENGINE = MergeTree() ORDER BY order_purchase_timestamp;

CREATE TABLE IF NOT EXISTS silver_order_items (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id String,
    shipping_limit_date DateTime,
    price Float64,
    freight_value Float64
) ENGINE = MergeTree() ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS silver_order_payments (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Float64
) ENGINE = MergeTree() ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS silver_order_reviews (
    review_id String,
    order_id String,
    review_score Int32,
    review_creation_date DateTime,
    review_answer_timestamp Nullable(DateTime)
) ENGINE = MergeTree() ORDER BY (review_id, order_id);


-- GOLD LAYER ( star schema )

-- A. The Fact Table
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id String,
    order_item_id Int32,
    customer_id String,
    product_id String,
    seller_id String,
    purchase_date_key Int32,
    approved_date_key Int32,
    delivered_carrier_date_key Int32,
    delivered_customer_date_key Int32,
    estimated_delivery_date_key Int32,
    order_status String,
    price Float64,
    freight_value Float64,
    payment_value_total Float64,
    payment_installments_max Int32,
    payment_type_main String,
    review_score Int32
) ENGINE = MergeTree() 
ORDER BY (purchase_date_key, order_id, order_item_id);

-- B. The Dimensions (Mapped as High-Performance Dictionaries)
-- These pull directly from Silver, meaning we don't have to duplicate data on disk

CREATE DICTIONARY IF NOT EXISTS dict_customers (
    customer_id String,
    customer_unique_id String,
    customer_city String,
    customer_state String
)
PRIMARY KEY customer_id
SOURCE(CLICKHOUSE(USER 'admin' PASSWORD 'admin' DB 'ecommerce_dw' TABLE 'silver_customers'))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY IF NOT EXISTS dict_sellers (
    seller_id String,
    seller_city String,
    seller_state String
)
PRIMARY KEY seller_id
SOURCE(CLICKHOUSE(USER 'admin' PASSWORD 'admin' DB 'ecommerce_dw' TABLE 'silver_sellers'))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY IF NOT EXISTS dict_products (
    product_id String,
    product_category_name_english String
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(USER 'admin' PASSWORD 'admin' DB 'ecommerce_dw' TABLE 'silver_products'))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE IF NOT EXISTS ecommerce_dw.dim_date (
    date_key Int32,
    full_date Date,
    year Int32,
    quarter Int32,
    month Int32,
    month_name String,
    week_of_year Int32,
    day_of_month Int32,
    day_of_week Int32,
    day_name String,
    is_weekend UInt8
) ENGINE = MergeTree() ORDER BY date_key;

CREATE DICTIONARY IF NOT EXISTS ecommerce_dw.dict_date (
    date_key UInt64, -- Dictionaries prefer UInt64 for numeric keys
    full_date Date,
    year Int32,
    quarter Int32,
    month Int32,
    month_name String,
    week_of_year Int32,
    day_of_month Int32,
    day_of_week Int32,
    day_name String,
    is_weekend UInt8
)
PRIMARY KEY date_key
SOURCE(CLICKHOUSE(USER 'admin' PASSWORD 'admin' DB 'ecommerce_dw' TABLE 'dim_date'))
LIFETIME(MIN 0 MAX 0) -- 0 means it never expires, because the calendar never changes!
LAYOUT(HASHED());


-- Seed the Date Dimension once on startup (10 Years starting from Jan 1, 2016)
INSERT INTO ecommerce_dw.dim_date
WITH toDate('2016-01-01') AS start_date
SELECT 
    toYYYYMMDD(start_date + number) AS date_key,
    start_date + number AS full_date,
    toYear(start_date + number) AS year,
    toQuarter(start_date + number) AS quarter,
    toMonth(start_date + number) AS month,
    
    -- Use 1-based array indexing to get the exact text names!
    ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'][toMonth(start_date + number)] AS month_name, 
    
    toISOWeek(start_date + number) AS week_of_year,
    toDayOfMonth(start_date + number) AS day_of_month,
    toDayOfWeek(start_date + number) AS day_of_week, 
    
    -- 1 = Monday, 7 = Sunday
    ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][toDayOfWeek(start_date + number)] AS day_name,   
    
    if(toDayOfWeek(start_date + number) IN (6, 7), 1, 0) AS is_weekend
FROM system.numbers
LIMIT 3650;