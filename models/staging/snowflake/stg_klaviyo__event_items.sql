with
    source as (

        select * from {{ ref('stg_klaviyo__events') }}
    )

    , event_items as (

        select
            events.event_id
            , flattened_item_properties.*
        from events
        , {{ flatten_json ('events', 'event_properties_metadata:Items') }} as flattened_items
        , {{ flatten_json ('flattened_event_properties', 'value') }} as flattened_item_properties
    )

    , pivoted_event_properties as (

        select
            events.event_id
            , flattened_item_properties
        from events
        left join
    )

    , final as (

        select
            event_id

            , type

            , updated_at -- TODO Fix this in the tap

            , {{ extract_json_field('event_properties_metadata', 'OrderId') }} as order_id
            , {{ extract_json_field('event_properties_metadata:Items', 'ProductID') }} as product_id
            , {{ extract_json_field('event_properties_metadata:Items', 'ProductName') }} as product_name
            , {{ extract_json_field('event_properties_metadata:Items', 'ProductURL') }} as product_url
            , {{ extract_json_field('event_properties_metadata:Items', 'SKU') }} as sku
            , {{ extract_json_field('event_properties_metadata:Items', 'Brand') }} as brand
            , {{ extract_json_field('event_properties_metadata:Items', 'Categories') }} as categories
            , {{ extract_json_field('event_properties_metadata:Items', 'ImageURL') }} as imageurl
            , {{ extract_json_field('event_properties_metadata:Items', 'ItemPrice') }} as item_price
            , {{ extract_json_field('event_properties_metadata:Items', 'Quantity') }} as quantity
            , {{ extract_json_field('event_properties_metadata:Items', 'RowTotal') }} as row_total

        from source

    )

select * from final