select
    ticket_id,
    user_id,
    created_at as ticket_created_at,
    resolved_at as ticket_resolved_at,
    status as ticket_status,
    priority as ticket_priority
from {{ source('staging', 'tickets') }}