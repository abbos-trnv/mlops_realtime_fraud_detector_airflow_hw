select
    weekday,
    transaction_hour,
    count() as transaction_count,
    sum(is_fraud) as fraud_count,
    100.0 * sum(is_fraud) / count() as fraud_rate,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from {{ ref('stg_transactions') }}
group by weekday, transaction_hour
