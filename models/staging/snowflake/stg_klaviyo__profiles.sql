{{
    config(
        enabled = target.type == 'postgres'
    )
}}


with source as (

    select * from {{ source('klaviyo', 'profiles') }}

)

, final as (

    select
        id as profile_id
        , type as profile_type
        , attributes::json->'email' as email
        , attributes::json->'image' as image
        , attributes::json->'title' as title
        , attributes::json->'created' as created_at
        , attributes::json->'first_name' as first_name
        , attributes::json->'last_name' as last_name
        , attributes::json->'properties' as properties
        , attributes::json->'external_id' as external_id
        , attributes::json->'organization' as organization
        , attributes::json->'phone_number' as phone_number
        , attributes::json->'last_event_date' as last_event_date
        , attributes::json->'location'->'zip' as zip_code
        , attributes::json->'location'->'city' as city
        , attributes::json->'location'->'region' as region
        , attributes::json->'location'->'country' as country
        , attributes::json->'location'->'address1' as address1
        , attributes::json->'location'->'address2' as address2
        , attributes::json->'location'->'latitude' as latitude
        , attributes::json->'location'->'timezone' as timezone
        , attributes::json->'location'->'longitude' as longitude
        , attributes::json->'subscriptions'->'sms'->'marketing'->'method' as sms_marketing_method
        , attributes::json->'subscriptions'->'sms'->'marketing'->'consent' as sms_marketing_consent
        , attributes::json->'subscriptions'->'sms'->'marketing'->'timestamp' as sms_marketing_timestampt
        , attributes::json->'subscriptions'->'sms'->'marketing'->'double_optin' as sms_marketing_double_optin
        , attributes::json->'subscriptions'->'sms'->'marketing'->'suppressions' as sms_marketing_suppressions
        , attributes::json->'subscriptions'->'sms'->'marketing'->'method_detail' as sms_marketing_method_detail
        , attributes::json->'subscriptions'->'sms'->'marketing'->'list_suppressions' as sms_marketing_list_suppressions
        , attributes::json->'subscriptions'->'sms'->'marketing'->'custom_method_detail' as sms_marketing_custom_method_detail
        , attributes::json->'subscriptions'->'email'->'marketing'->'method' as email_marketing_method
        , attributes::json->'subscriptions'->'email'->'marketing'->'consent' as email_marketing_consent
        , attributes::json->'subscriptions'->'email'->'marketing'->'timestamp' as email_marketing_timestampt
        , attributes::json->'subscriptions'->'email'->'marketing'->'double_optin' as email_marketing_double_optin
        , attributes::json->'subscriptions'->'email'->'marketing'->'suppressions' as email_marketing_suppressions
        , attributes::json->'subscriptions'->'email'->'marketing'->'method_detail' as email_marketing_method_detail
        , attributes::json->'subscriptions'->'email'->'marketing'->'list_suppressions' as email_marketing_list_suppressions
        , attributes::json->'subscriptions'->'email'->'marketing'->'custom_method_detail' as email_marketing_custom_method_detail
        , updated as updated_at -- TODO Fix this in the tap

    from source

)

select * from final