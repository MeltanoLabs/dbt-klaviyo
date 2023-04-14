with source as (

    select * from {{ source('klaviyo', 'lists') }}

)

, final as (

{% if target.type == 'postgres'%}

    select
        id as event_id
        , attributes::json->'name' as list_name
        , attributes::json->'created' as created_at
        , updated as updated

{% elif target.type == 'snowflake'%}

    select
        id::string as event_id
        , attributes:name::string as list_name
        , attributes:created::timestamptz as created_at
        , updated::timestamptz as updated

{% else %}

    select *

{% endif %}

    from source

)

select * from final