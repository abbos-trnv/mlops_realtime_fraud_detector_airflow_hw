select
    merchant,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    sum(is_fraud) as fraud_count,
    100.0 * sum(is_fraud) / count() as fraud_rate,
    toUInt8(count() >= 20 and 100.0 * sum(is_fraud) / count() >= 5) as is_suspicious
from {{ ref('stg_transactions') }}
group by merchant
