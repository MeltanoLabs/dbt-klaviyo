with source as (

    select * from {{ source('klaviyo', 'flows') }}

)

, final as (

    select
        id::string as id
        , attributes:name::string as name
        , attributes:status::string as status
        , attributes:created::timestamptz as created
        , attributes:updated::timestamptz as updated
        , attributes:trigger_type::string as "trigger"
        , attributes:archived::boolean as archived
    from source

)

select * from final