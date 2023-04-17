{{
    config(
        enabled = target.type == 'snowflake'
    )
}}


with source as (

    select * from {{ source('snowflake_klaviyo', 'campaigns') }}

)

, final as (
    select
        id as campaign_id
        , updated_at::timestamp
        , attributes:name as campaign_name
        , attributes:type as type
        , attributes:status as status
        , attributes:channel as channel
        , attributes:message as message
        , attributes:archived as is_archived
        , attributes:send_time as send_time
        , attributes:created_at as created_at
        , attributes:scheduled_at as scheduled_at
    from source
)

select * from final