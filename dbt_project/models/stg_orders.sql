select
    order_id,
    user_id,
    order_date,
    amount as order_amount,
    status as order_status
from {{ source('staging', 'orders') }}