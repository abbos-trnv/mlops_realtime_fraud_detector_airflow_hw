with normalized as (
    select
        toDateTime(transaction_time, 'UTC') as transaction_time,
        trimBoth(merch) as merchant,
        lowerUTF8(trimBoth(cat_id)) as category,
        toFloat64(amount) as amount,
        trimBoth(name_1) as first_name,
        trimBoth(name_2) as last_name,
        upperUTF8(trimBoth(gender)) as gender,
        trimBoth(street) as street,
        upperUTF8(trimBoth(us_state)) as state,
        toUInt8(target) as is_fraud
    from {{ source('raw', 'transactions') }}
)

select
    transaction_time,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    {{ dbt_date.day_of_week('transaction_time', isoweek=true) }} as weekday,
    merchant,
    category,
    amount,
    gender,
    state,
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'street', 'state']) }} as customer_id,
    is_fraud,
    {{ amount_bucket('amount') }} as amount_bucket
from normalized
