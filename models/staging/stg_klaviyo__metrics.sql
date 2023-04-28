with source as (

    select * from {{ source('klaviyo', 'metrics') }}

)

, final as (

    select
        id as metric_id
        , attributes:name::string as metric_name
        , attributes:created::timestamptz as created_at
        , attributes:integration:id::string as integration_id
        , attributes:integration:name::string as integration_name
        , attributes:integration:category::string as integration_category
        , updated::timestamptz as updated
    from source

)

select * from final