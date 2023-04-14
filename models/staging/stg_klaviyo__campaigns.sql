with source as (

    select * from {{ source('klaviyo', 'campaigns') }}

)

, final as (
    {% if target.type == 'postgres'%}
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

        {% elif target.type == 'snowflake'%}

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

        {% else %}

            select *

        {% endif %}

    from source

)

select * from final