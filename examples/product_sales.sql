-- Example: Product sales with join
-- Dimension changes (product.name) don't affect aggregate, so deduplication helps

CREATE DYNAMIC TABLE lake.dynamic.product_sales
TARGET_LAG = '10 minutes'
DEDUPLICATION = true
AS
SELECT 
    ol.product_id,
    SUM(ol.quantity) as total_quantity,
    SUM(ol.quantity * ol.price) as total_revenue,
    COUNT(DISTINCT ol.order_id) as order_count
FROM lake.orderlines ol
JOIN lake.products p ON ol.product_id = p.product_id
GROUP BY ol.product_id;
