with source as (

    select * from {{ source('klaviyo', 'events') }}

)

, final as (

    select
        id
        , attributes:uuid::string as uuid
        , attributes:metric_id::string as metric_id
        , attributes:profile_id::string as profile_id
        , type
        , datetime::timestamptz as updated_at
        , attributes:timestamp::timestamptz as timestamp
        , attributes:event_properties as event_properties_metadata
        , attributes as attributes_metadata
    from source

)

select * from final