select sum(transaction_count) as mart_count
from {{ ref('mart_fraud_by_category') }}
having mart_count != (select count() from {{ ref('stg_transactions') }})
