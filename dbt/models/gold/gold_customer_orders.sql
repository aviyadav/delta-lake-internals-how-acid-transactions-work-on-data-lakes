{{ config(materialized='table', file_format='delta') }}

-- Customer-level summary built from the Silver Delta table.
select
    customer_id,
    count(*)                                                              as total_orders,
    sum(order_amount)                                                     as lifetime_value,
    sum(case when order_status = 'COMPLETED' then order_amount else 0 end) as realized_revenue,
    count(case when order_status = 'CANCELLED' then 1 end)                as cancelled_orders,
    min(order_date)                                                       as first_order_date,
    max(order_date)                                                       as last_order_date,
    max(updated_at)                                                       as last_order_at
from {{ source('lakehouse', 'orders') }}
group by customer_id
