with source as (

    select * from {{ source('snowflake_klaviyo', 'events') }}

)

, final as (

    select
        id as event_id

        , type

        , datetime::timestamp as updated_at

        , json_extract_path_text(attributes::variant, 'datetime') as datetime
        , json_extract_path_text(attributes::variant, 'timestamp') as timestamp

        , json_extract_path_text(attributes::variant, 'uuid') as uuid
        , json_extract_path_text(attributes::variant, 'metric_id') as metric_id
        , json_extract_path_text(attributes::variant, 'profile_id') as profile_id

        , attributes::variant:event_properties as event_properties_metadata
        , attributes as attributes_metadata
    from source

)

select * from final