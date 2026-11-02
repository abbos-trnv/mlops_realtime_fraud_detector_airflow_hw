select
    'high' as customer_id,
    toDateTime('2020-01-01 00:00:00', 'UTC') as transaction_time,
    toFloat64(100) as amount,
    toUInt8(number < 5) as is_fraud
from numbers(100)
union all
select
    'medium' as customer_id,
    toDateTime('2020-01-01 00:00:00', 'UTC') as transaction_time,
    toFloat64(100) as amount,
    toUInt8(number < 1) as is_fraud
from numbers(100)
union all
select
    'low' as customer_id,
    toDateTime('2020-01-01 00:00:00', 'UTC') as transaction_time,
    toFloat64(100) as amount,
    toUInt8(0) as is_fraud
from numbers(100)
