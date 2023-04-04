with source as (

    select * from {{ source('klaviyo', 'metrics') }}

)

, final as (

    select
        id as metric_id
        , attributes::json->'name' as metric_name
        , attributes::json->'created' as created_at
        , attributes::json->'integration'->'id' as integration_id
        , attributes::json->'integration'->'name' as integration_name
        , attributes::json->'integration'->'category' as integration_category
        , updated as updated_at -- TODO Fix this in the tap

    from source

)

select * from final