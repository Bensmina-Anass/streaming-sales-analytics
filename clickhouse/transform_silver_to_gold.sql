-- Build the Central Fact Table
TRUNCATE TABLE IF EXISTS ecommerce_dw.fact_order_items;
INSERT INTO ecommerce_dw.fact_order_items
SELECT 
    i.order_id, 
    i.order_item_id, 
    o.customer_id, 
    i.product_id, 
    i.seller_id,
    
    -- Date Dimension Keys (coalesce to 0 for missing/null dates)
    toYYYYMMDD(o.order_purchase_timestamp) AS purchase_date_key,
    coalesce(toYYYYMMDD(o.order_approved_at), 0) AS approved_date_key,
    coalesce(toYYYYMMDD(o.order_delivered_carrier_date), 0) AS delivered_carrier_date_key,
    coalesce(toYYYYMMDD(o.order_delivered_customer_date), 0) AS delivered_customer_date_key,
    coalesce(toYYYYMMDD(o.order_estimated_delivery_date), 0) AS estimated_delivery_date_key,
    
    o.order_status,
    i.price,
    i.freight_value,
    
    -- Aggregated Payment and Review Data
    coalesce(p.payment_value_total, 0) AS payment_value_total,
    coalesce(p.payment_installments_max, 1) AS payment_installments_max,
    coalesce(p.payment_type_main, 'unknown') AS payment_type_main,
    
    -- Cast the average review to Int32 to match the Fact table schema
    CAST(round(coalesce(r.review_score, 0)) AS Int32) AS review_score

FROM ecommerce_dw.silver_order_items i
JOIN ecommerce_dw.silver_orders o ON i.order_id = o.order_id

-- Subquery to aggregate multiple payments per order
LEFT JOIN (
    SELECT 
        order_id, 
        sum(payment_value) AS payment_value_total, 
        max(payment_installments) AS payment_installments_max,
        any(payment_type) AS payment_type_main
    FROM ecommerce_dw.silver_order_payments 
    GROUP BY order_id
) p ON i.order_id = p.order_id

-- Subquery to aggregate multiple reviews per order
LEFT JOIN (
    SELECT 
        order_id, 
        avg(review_score) AS review_score 
    FROM ecommerce_dw.silver_order_reviews 
    GROUP BY order_id
) r ON i.order_id = r.order_id;