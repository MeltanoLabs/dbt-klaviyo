with source as (

    select * from {{ source('klaviyo', 'events') }}

)

, final as (

        {% if target.type == 'postgres'%}

            select
                id as event_id

                , type

                , datetime::timestamp as updated_at -- TODO Fix this in the tap

                , attributes::json->'uuid' as uuid
                , attributes::json->'datetime' as datetime
                , attributes::json->'metric_id' as metric_id
                , attributes::json->'timestamp' as timestamp
                , attributes::json->'profile_id' as profile_id

                , event_properties::json->'ProductName' as product_name
                , event_properties::json->'ProductID' as product_id
                , event_properties::json->'Categories' as categories
                , event_properties::json->'ImageURL' as image_url
                , event_properties::json->'Brand' as brand
                , event_properties::json->'Price' as price
                , event_properties::json->'CompareAtPrice' as compare_at_price

        {% elif target.type == 'snowflake'%}

            select
                id as event_id

                , type

                , datetime::timestamp as updated_at -- TODO Fix this in the tap

                , attributes:uuid as uuid
                , attributes:datetime as datetime
                , attributes:metric_id as metric_id
                , attributes:timestamp as timestamp
                , attributes:profile_id as profile_id

                , event_properties:ProductName as product_name
                , event_properties:ProductID as product_id
                , event_properties:Categories as categories
                , event_properties:ImageURL as image_url
                , event_properties:Brand as brand
                , event_properties:Price as price
                , event_properties:CompareAtPrice as compare_at_price
                , event_properties:$event_id as event_id

        {% else %}

            select *

        {% endif %}


    from source

)

select * from final