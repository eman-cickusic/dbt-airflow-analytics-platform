with users as (
    select * from {{ ref('stg_users') }}
),
tickets as (
    select
        user_id,
        count(ticket_id) as total_support_tickets
    from {{ ref('stg_tickets') }}
    group by 1
)
select
    u.user_id,
    u.user_name,
    u.user_email,
    u.user_created_at,
    coalesce(t.total_support_tickets, 0) as total_support_tickets
from users u
left join tickets t on u.user_id = t.user_id