with aggregated as (
    select
        customer_id,
        count() as transaction_count,
        min(transaction_time) as first_transaction_at,
        max(transaction_time) as last_transaction_at,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        sum(amount) as total_amount,
        avg(amount) as avg_amount
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    *,
    {{ risk_level('fraud_rate') }} as risk_level
from aggregated
