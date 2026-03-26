## Watch  command for postres (view live change):


watch -n 5 "docker exec oltp_postgres psql -U admin -d \"e-commerce\" -c 'SELECT (SELECT COUNT(*) FROM orders) AS orders, (SELECT COUNT(*) FROM order_items) AS order_items, (SELECT COUNT(*) FROM order_payments) AS order_payments, (SELECT COUNT(*) FROM order_reviews) AS order_reviews;'"




## Watch command for clickhouse:

watch -n 5 "docker exec olap_clickhouse clickhouse-client \
  --user admin --password admin \
  --query \"
    SELECT 'bronze_orders'          AS table, count() AS rows FROM ecommerce_dw.bronze_orders
    UNION ALL SELECT 'bronze_order_items',       count() FROM ecommerce_dw.bronze_order_items
    UNION ALL SELECT 'bronze_order_payments',    count() FROM ecommerce_dw.bronze_order_payments
    UNION ALL SELECT 'bronze_order_reviews',     count() FROM ecommerce_dw.bronze_order_reviews
  \""


# Run each one in a seperate terminal