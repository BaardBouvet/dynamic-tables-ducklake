-- Example: Downstream table with 'downstream' lag
-- Refreshes whenever parent (customer_metrics) refreshes

CREATE DYNAMIC TABLE lake.dynamic.high_value_customers
TARGET_LAG = 'downstream'
AS
SELECT 
    customer_id,
    order_count,
    total_amount
FROM lake.dynamic.customer_metrics
WHERE total_amount > 10000;
