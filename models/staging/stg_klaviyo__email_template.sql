with source as (

    select * from {{ source('klaviyo', 'templates') }}

)

, final as (

    select
        id::string as id
        , attributes:name::string as name
        , attributes:html::string as html
        , attributes:created::timestamptz as created
        , updated::timestamptz as updated
    from source

)

select * from final