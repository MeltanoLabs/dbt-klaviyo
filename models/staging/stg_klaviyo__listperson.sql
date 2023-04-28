with source as (

    select * from {{ source('klaviyo', 'listperson') }}

)

, final as (

    select
        id as person_id
        , list_id as list_id
    from source

)

select * from final