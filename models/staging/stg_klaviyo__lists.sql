with source as (

    select * from {{ source('klaviyo', 'lists') }}

)

, final as (

    select
        id::string as id
        , attributes:name::string as list_name
        , attributes:created::timestamptz as created
        , updated::timestamptz as updated
    from source

)

select * from final