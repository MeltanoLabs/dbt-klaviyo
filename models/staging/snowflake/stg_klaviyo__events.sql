with source as (

    select * from {{ source('snowflake_klaviyo', 'events') }}

)

, final as (

    select
        id as event_id

        , type

        , datetime::timestamp as updated_at -- TODO Fix this in the tap

        , {{ extract_json_field('attributes', 'uuid') }} as uuid
        , {{ extract_json_field('attributes', 'metric_id') }} as metric_id
        , {{ extract_json_field('attributes', 'profile_id') }} as profile_id

        , {{ extract_json_field('attributes', 'datetime') }} as datetime
        , {{ extract_json_field('attributes', 'timestamp') }} as timestamp


        , {{ extract_json_field('attributes', 'event_properties') }} as event_properties
        , {{ extract_json('attributes:event_properties') }} as event_properties_metadata
        , attributes as attributes_metadata

    from source

)

select * from final