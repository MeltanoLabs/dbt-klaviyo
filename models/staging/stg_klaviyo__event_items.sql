with
    events as (

        select * from {{ ref('stg_klaviyo__events') }}
    )

    , event_properties as (
        select
            events.id as event_id
            , events.event_properties_metadata:OrderId
            , events.event_properties_metadata:ProductID as product_id
            , flattened_properties.*
        from events
        , lateral flatten(input => events.event_properties_metadata) flattened_properties
    )

    , event_items as (
        select
            events.id as event_id
            , events.event_properties_metadata:OrderId as order_id
            , flattened_item_metadata.value:ProductID as product_id
            , flattened_items.*
        from events
        , lateral flatten(input => events.event_properties_metadata:Items) flattened_item_metadata
        , lateral flatten(input => flattened_item_metadata.value) flattened_items
    )

    , single_item_events as (
        select
            event_properties.event_id
            , event_properties.product_id
            , 1 as quantity
            , max(case when event_properties.key = 'ImageURL' then event_properties.value end) as image_url
            , max(case when event_properties.key = 'Price' then event_properties.value end) as item_price
            , max(case when event_properties.key = 'ProductCategories' then event_properties.value end) as product_categories
            , max(case when event_properties.key = 'ProductName' then event_properties.value end) as product_name
            , max(case when event_properties.key = 'ProductURL' then event_properties.value end) as product_url
            , max(case when event_properties.key = 'SKU' then event_properties.value end) as sku
            , max(case when event_properties.key = 'Brand' then event_properties.value end) as brand
            , max(case when event_properties.key = 'Categories' then event_properties.value end) as categories
        from event_properties
        left join event_items
            on event_properties.event_id = event_items.event_id
        where
            event_items.event_id is null /* Exclude events with multiple items */
            and event_properties.product_id is not null /* Exclude events with no items */
        group by 1, 2, 2
    )

    , multiple_item_events as (
        select
            event_id
            , product_id
            , max(case when key = 'Quantity' then value end) as quantity
            , max(case when key = 'ImageURL' then value end) as image_url
            , max(case when key = 'ItemPrice' then value end) as item_price
            , max(case when key = 'ProductCategories' then value end) as product_categories
            , max(case when key = 'ProductName' then value end) as product_name
            , max(case when key = 'ProductURL' then value end) as product_url
            , max(case when key = 'SKU' then value end) as sku
            , max(case when key = 'Brand' then value end) as brand
            , max(case when key = 'Categories' then value end) as categories
        from event_items
        group by 1, 2
    )

    , unioned_event_items as (
        select * from single_item_events
        union all
        select * from multiple_item_events
    )

select
    event_id
    , product_id
    , quantity::number as quantity
    , image_url::string as image_url
    , item_price::decimal(38,2) as item_price
    , product_categories::string as product_categories
    , product_name::string as product_name
    , product_url::string as product_url
    , sku::string as sku
    , brand::string as brand
    , categories
from unioned_event_items