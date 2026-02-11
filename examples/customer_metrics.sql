-- Example: Customer metrics dynamic table with deduplication
-- Usage: dynamic-tables create -f customer_metrics.sql

CREATE DYNAMIC TABLE lake.dynamic.customer_metrics
TARGET_LAG = '5 minutes'
DEDUPLICATION = true
AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(order_date) as last_order_date
FROM lake.orders
GROUP BY customer_id;
