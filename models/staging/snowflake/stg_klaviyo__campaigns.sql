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

        , json_extract_path_text(attributes::variant, 'created_at') as created_at
        , json_extract_path_text(attributes::variant, 'scheduled_at') as scheduled_at
        , json_extract_path_text(attributes::variant, 'name') as campaign_name
        , json_extract_path_text(attributes::variant, 'type') as type
        , json_extract_path_text(attributes::variant, 'status') as status
        , json_extract_path_text(attributes::variant, 'channel') as channel
        , json_extract_path_text(attributes::variant, 'message') as message
        , json_extract_path_text(attributes::variant, 'archived') as is_archived
        , json_extract_path_text(attributes::variant, 'send_time') as send_time

        , attributes as attributes_metadata

    from source
)
select * from final