{{
    config(
        enabled = target.type == 'snowflake'
    )
}}

with source as (

    select * from {{ source('klaviyo', 'campaigns') }}

)

, final as (
    select
        id
        , updated_at::timestamptz as updated_at
        , attributes:created_at::timestamptz as created_at
        , attributes:scheduled_at::timestamptz as scheduled_at
        , attributes:send_time::timestamptz as send_time
        , attributes:name::string as campaign_name
        , attributes:type::string as type
        , attributes:status::string as status
        , attributes:channel::string as channel
        , attributes:message::string as message
        , attributes:archived::boolean as is_archived
        , attributes as attributes_metadata

    from source
)
select * from final