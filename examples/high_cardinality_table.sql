-- Example: High-cardinality table with custom thresholds
-- Tuned for large datasets with many affected keys

CREATE DYNAMIC TABLE lake.dynamic.user_events_summary
TARGET_LAG = '10 minutes'
CARDINALITY_THRESHOLD = 0.5
PARALLEL_THRESHOLD = 5000000
MAX_PARALLELISM = 8
AS
SELECT 
    user_id,
    COUNT(*) as event_count,
    SUM(value) as total_value,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event
FROM lake.events
GROUP BY user_id;
