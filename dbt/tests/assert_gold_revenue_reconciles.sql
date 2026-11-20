-- Singular test: Gold must reconcile exactly with Silver COMPLETED revenue.
with silver as (
    select coalesce(sum(order_amount), 0) as revenue
    from {{ source('lakehouse', 'orders') }}
    where order_status = 'COMPLETED'
),
gold as (
    select coalesce(sum(realized_revenue), 0) as revenue
    from {{ ref('gold_daily_revenue') }}
)
select
    silver.revenue as silver_revenue,
    gold.revenue   as gold_revenue
from silver
cross join gold
where abs(silver.revenue - gold.revenue) > 0.01
