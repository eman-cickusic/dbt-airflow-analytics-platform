select
    user_id,
    name as user_name,
    email as user_email,
    address as user_address,
    created_at as user_created_at
from {{ source('staging', 'users') }}
