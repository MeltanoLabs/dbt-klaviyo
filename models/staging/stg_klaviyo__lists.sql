with source as (

    select * from {{ source('klaviyo', 'lists') }}

)

, final as (

    select
        id::string as event_id
        , attributes:name::string as list_name
        , attributes:created::timestamptz as created_at
        , updated::timestamptz as updated
    from source

)

select * from final