-- Trino queries against the same Delta files Spark wrote.
--   docker compose exec -T trino trino --catalog delta --schema gold
-- or paste into http://localhost:8085

-- 1. Which catalogs / schemas did the metastore expose?
SHOW SCHEMAS FROM delta;

-- 2. The cross-engine proof: Spark wrote, Trino reads.
SELECT count(*) AS orders,
       count(DISTINCT customer_id) AS customers,
       min(updated_at) AS first_order,
       max(updated_at) AS last_order
FROM delta.silver.orders;

-- 3. Business rollups built by dbt (Spark SQL) but queried here.
SELECT order_date,
       total_orders,
       active_customers,
       gross_revenue,
       realized_revenue
FROM delta.gold.gold_daily_revenue
ORDER BY order_date DESC;

SELECT customer_id,
       total_orders,
       lifetime_value,
       realized_revenue
FROM delta.gold.gold_customer_orders
ORDER BY lifetime_value DESC
LIMIT 10;

-- 4. What the quality gate recorded on its last run.
SELECT checked_at, check_name, severity, passed, observed
FROM delta.gold.dq_results
ORDER BY checked_at DESC
LIMIT 20;

-- 5. Quarantined source rows are visible, not silently dropped.
SELECT _reject_reason, count(*) AS rows
FROM delta.silver.orders_rejected
GROUP BY 1
ORDER BY rows DESC;

-- 6. Time travel is a reader feature: same table, older snapshot.
SELECT count(*) FROM delta.silver.orders FOR VERSION AS OF 1;

-- 7. Schema discovery for the Gold contracts.
DESCRIBE delta.gold.gold_customer_orders;
