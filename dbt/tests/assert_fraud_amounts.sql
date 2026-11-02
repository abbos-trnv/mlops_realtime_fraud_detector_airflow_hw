select category
from {{ ref('mart_fraud_by_category') }}
where fraud_amount > total_amount or fraud_count > transaction_count
