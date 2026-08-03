with source_data as (
    select * from {{ source('raw_source', 'raw_transactions') }}
)

select
    transaction_id,
    customer_name,
    initcap(product_category) as product_category,  -- Standardizes category text
    price,
    quantity,
    (price * quantity) as total_order_value,        -- Calculated metric
    case 
        when (price * quantity) > 300 then true 
        else false 
    end as is_high_value_deal,                      -- Data engineer feature flag
    payment_method,
    cast(timestamp as timestamp) as transaction_time
from source_data
