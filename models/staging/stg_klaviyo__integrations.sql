with source as (

    select * from {{ ref('stg_klaviyo__metrics') }}

)

, final as (

    select distinct
        attributes_metadata:integration:id::string as id
        , attributes_metadata:integration:category::string as category
        , attributes_metadata:integration:name::string as name
    from source

)

select * from final