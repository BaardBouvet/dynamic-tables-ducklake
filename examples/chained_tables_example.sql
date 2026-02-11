-- Example: Chained Dynamic Tables
-- Dependency graph:
--   orders → customer_totals → premium_customer_insights
--   orders ----------------→ premium_customer_insights
--
-- This demonstrates:
-- 1. Table dependencies and refresh ordering
-- 2. 'downstream' lag propagation
-- 3. Multiple parents (premium_customer_insights uses both customer_totals and orders)

-- Step 1: Base aggregation
-- Aggregates order data by customer
CREATE DYNAMIC TABLE lake.dynamic.customer_totals
TARGET_LAG = '10 minutes'
DEDUPLICATION = true
AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    MAX(order_date) as last_order_date
FROM lake.orders
GROUP BY customer_id;

-- Step 2: Intermediate derived table
-- Uses customer_totals to identify premium customers
-- Refreshes whenever customer_totals refreshes
CREATE DYNAMIC TABLE lake.dynamic.premium_customers
TARGET_LAG = 'downstream'
AS
SELECT 
    customer_id,
    order_count,
    total_spent,
    avg_order_value,
    last_order_date
FROM lake.dynamic.customer_totals
WHERE total_spent > 10000;

-- Step 3: Advanced analysis table
-- Uses BOTH customer_totals (for filtering) AND orders (for detailed breakdown)
-- Refreshes when EITHER parent refreshes
CREATE DYNAMIC TABLE lake.dynamic.premium_customer_insights
TARGET_LAG = 'downstream'
AS
SELECT 
    ct.customer_id,
    ct.total_spent,
    ct.order_count,
    -- Detailed breakdown from orders
    COUNT(DISTINCT DATE_TRUNC('month', o.order_date)) as active_months,
    SUM(CASE WHEN o.order_date >= CURRENT_DATE - INTERVAL '30 days' THEN o.amount ELSE 0 END) as last_30_days_spent,
    ARRAY_AGG(DISTINCT o.product_category) as purchased_categories
FROM lake.dynamic.customer_totals ct
JOIN lake.orders o ON ct.customer_id = o.customer_id
WHERE ct.total_spent > 10000
GROUP BY ct.customer_id, ct.total_spent, ct.order_count;

-- Refresh behavior:
-- 1. When orders changes → customer_totals refreshes first
-- 2. Then premium_customers refreshes (downstream of customer_totals)
-- 3. Then premium_customer_insights refreshes (downstream of BOTH parents)
-- All refreshes happen in same iteration (topological order)
