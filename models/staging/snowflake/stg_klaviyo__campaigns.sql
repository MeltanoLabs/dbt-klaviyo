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

        , {{ extract_json_field('attributes', 'created_at') }} as created_at
        , {{ extract_json_field('attributes', 'scheduled_at') }} as scheduled_at
        , {{ extract_json_field('attributes', 'name') }} as campaign_name
        , {{ extract_json_field('attributes', 'type') }} as type
        , {{ extract_json_field('attributes', 'status') }} as status
        , {{ extract_json_field('attributes', 'channel') }} as channel
        , {{ extract_json_field('attributes', 'message') }} as message
        , {{ extract_json_field('attributes', 'archived') }} as is_archived
        , {{ extract_json_field('attributes', 'send_time') }} as send_time

        , attributes as attributes_metadata

    from source
)
select * from final