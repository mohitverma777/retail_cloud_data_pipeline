with source_data as (
    select * from {{ source('raw_source', 'raw_unstructured_chats') }}
)

select
    chat_id,
    cast(timestamp as timestamp) as chat_logged_time,
    
    -- Snowflake Syntax: Using ':' to parse keys directly from raw JSON Variant payload
    cast(raw_chat_payload:text_log as varchar) as raw_chat_text,
    cast(raw_chat_payload:origin_device as varchar) as mobile_os_platform
from source_data
