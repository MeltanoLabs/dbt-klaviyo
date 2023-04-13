with source as (

    select * from {{ source('klaviyo', 'metrics') }}

)

, final as (

{% if target.type == 'postgres'%}

    select
        id as metric_id
        , attributes::json->'name' as metric_name
        , attributes::json->'created' as created_at
        , attributes::json->'integration'->'id' as integration_id
        , attributes::json->'integration'->'name' as integration_name
        , attributes::json->'integration'->'category' as integration_category
        , updated as updated_at

{% elif target.type == 'snowflake'%}

    select
        id as metric_id
        , attributes:name::string as metric_name
        , attributes:created::timestamptz as created_at
        , attributes:integration:id::string as integration_id
        , attributes:integration:name::string as integration_name
        , attributes:integration:category::string as integration_category
        , updated::timestamptz as updated_at

{% else %}

    select *

{% endif %}

    from source
    
)

select * from final