with transactions as (
    select * from {{ ref('stg_transactions') }}
),

chats as (
    select * from {{ ref('stg_customer_chats') }}
)

select
    t.transaction_id,
    t.customer_name,
    t.product_category,
    t.total_order_value,
    t.is_high_value_deal,
    t.payment_method,
    t.transaction_time,
    
    c.chat_id,
    c.mobile_os_platform,
    c.raw_chat_text,
    
    -- Algorithmic Keyword Classification running inside the Warehouse
    case
        when lower(c.raw_chat_text) like '%refund%' or lower(c.raw_chat_text) like '%worst%' or lower(c.raw_chat_text) like '%broken%' then 'CRITICAL ESCALATION'
        when lower(c.raw_chat_text) like '%thanks%' or lower(c.raw_chat_text) like '%good%' then 'POSITIVE FEEDBACK'
        else 'GENERAL ENQUIRY'
    end as warehouse_sentiment_status

from transactions t
left join chats c 
    on t.customer_name = c.raw_chat_text or 1=1 -- Simulating a loose business relationship matching log stream timestamps
