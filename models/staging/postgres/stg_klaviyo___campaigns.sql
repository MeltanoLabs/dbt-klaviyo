{{
    config(
        enabled = target.type == 'postgres'
    )
}}



with source as (

    select * from {{ source('postgres_klaviyo', 'campaigns') }}

)

, final as (
    select
        id as campaign_id
        , updated_at::timestamp
        , attributes::json->'name' as campaign_name
        , attributes::json->'type' as type
        , attributes::json->'status' as status
        , attributes::json->'channel' as channel
        , attributes::json->'message' as message
        , attributes::json->'archived' as is_archived
        , attributes::json->'send_time' as send_time
        , attributes::json->'created_at' as created_at
        , attributes::json->'scheduled_at' as scheduled_at
    from source

)

select * from final