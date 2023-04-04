with source as (

    select * from {{ source('klaviyo', 'lists') }}

)

, final as (

    select
        id as event_id
        , attributes::json->'name' as list_name
        , attributes::json->'created' as created_at
        , updated as updated_at -- TODO Fix this in the tap

    from source

)

select * from final