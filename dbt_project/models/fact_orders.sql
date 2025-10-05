select
    order_id,
    user_id,
    order_date,
    order_amount
from {{ ref('stg_orders') }}
where order_status = 'completed' 