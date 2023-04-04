with source as (

    select * from {{ source('klaviyo', 'campaigns') }}

)

, final as (

    select
        id as campaign_id
        , attributes::json->'name' as campaign_name
        , attributes::json->'status' as status
        , attributes::json->'channel' as channel
        , attributes::json->'message' as message
        , attributes::json->'archived' as is_archived
        , attributes::json->'send_time' as send_time
        , attributes::json->'created_at' as created_at
        , attributes::json->'scheduled_at' as scheduled_at
        , updated_at::timestamp 

    from source

)

select * from final