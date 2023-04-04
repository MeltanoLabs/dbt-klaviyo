with source as (

    select * from {{ source('klaviyo', 'events') }}

)

, final as (

    select
        id as event_id
        , attributes::json->'uuid' as uuid
        , attributes::json->'datetime' as datetime
        , attributes::json->'metric_id' as metric_id
        , attributes::json->'timestamp' as timestamp
        , attributes::json->'profile_id' as profile_id
        , null as event_properties -- TODO parse this structure
        , datetime::timestamp as updated_at -- TODO Fix this in the tap

    from source

)

select * from final