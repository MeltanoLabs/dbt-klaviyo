with source as (

    select * from {{ source('klaviyo', 'metrics') }}

)

, final as (

    select
        id
        , attributes:name::string as name
        , attributes:integration:id::string as integration_id
        , attributes:integration:name::string as integration_name
        , attributes:integration:category::string as integration_category
        , attributes:created::timestamptz as created
        , updated::timestamptz as updated
        , attributes as attributes_metadata
    from source

)

select * from final