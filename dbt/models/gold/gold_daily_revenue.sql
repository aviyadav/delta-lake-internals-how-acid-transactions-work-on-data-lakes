{{ config(materialized='table', file_format='delta') }}

select
    order_date,
    count(*)                                                              as total_orders,
    count(distinct customer_id)                                           as active_customers,
    sum(order_amount)                                                     as gross_revenue,
    sum(case when order_status = 'COMPLETED' then order_amount else 0 end) as realized_revenue,
    sum(case when order_status = 'CANCELLED' then order_amount else 0 end) as cancelled_value
from {{ source('lakehouse', 'orders') }}
group by order_date
