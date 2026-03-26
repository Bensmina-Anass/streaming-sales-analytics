-- 1. Process Customers (Map zip codes to latitude/longitude)
TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_customers;
INSERT INTO ecommerce_dw.silver_customers
SELECT 
    c.customer_id, 
    c.customer_unique_id, 
    c.customer_zip_code_prefix, 
    c.customer_city, 
    c.customer_state,
    any(g.geolocation_lat) AS customer_lat, 
    any(g.geolocation_lng) AS customer_lng
FROM ecommerce_dw.bronze_customers c
LEFT JOIN ecommerce_dw.bronze_geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
GROUP BY c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state;

-- 2. Process Sellers (Map zip codes to latitude/longitude)
TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_sellers;
INSERT INTO ecommerce_dw.silver_sellers
SELECT 
    s.seller_id, 
    s.seller_zip_code_prefix, 
    s.seller_city, 
    s.seller_state,
    any(g.geolocation_lat) AS seller_lat, 
    any(g.geolocation_lng) AS seller_lng
FROM ecommerce_dw.bronze_sellers s
LEFT JOIN ecommerce_dw.bronze_geolocation g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
GROUP BY s.seller_id, s.seller_zip_code_prefix, s.seller_city, s.seller_state;

-- 3. Process Products (Translate categories to English)
TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_products;
INSERT INTO ecommerce_dw.silver_products
SELECT 
    p.product_id, 
    coalesce(t.product_category_name_english, p.product_category_name),
    p.product_weight_g, 
    p.product_length_cm, 
    p.product_height_cm, 
    p.product_width_cm
FROM ecommerce_dw.bronze_products p
LEFT JOIN ecommerce_dw.bronze_category_translation t ON p.product_category_name = t.product_category_name;

-- 4. Pass-through Transactional Tables
TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_orders;
INSERT INTO ecommerce_dw.silver_orders SELECT * FROM ecommerce_dw.bronze_orders;

TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_order_items;
INSERT INTO ecommerce_dw.silver_order_items SELECT * FROM ecommerce_dw.bronze_order_items;

TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_order_payments;
INSERT INTO ecommerce_dw.silver_order_payments SELECT * FROM ecommerce_dw.bronze_order_payments;

-- Drop the heavy text comment columns to keep the Silver layer analytical and fast
TRUNCATE TABLE IF EXISTS ecommerce_dw.silver_order_reviews;
INSERT INTO ecommerce_dw.silver_order_reviews 
SELECT 
    review_id, 
    order_id, 
    review_score, 
    review_creation_date, 
    review_answer_timestamp 
FROM ecommerce_dw.bronze_order_reviews;