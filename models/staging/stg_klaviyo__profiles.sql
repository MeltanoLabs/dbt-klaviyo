with source as (

    select * from {{ source('klaviyo', 'profiles') }}

)

, final as (

    select
        id as profile_id
        , type as profile_type
        , attributes:email::string as email
        , attributes:image::string as image
        , attributes:title::string as title
        , attributes:created::timestamptz as created
        , attributes:first_name::string as first_name
        , attributes:last_name::string as last_name
        , attributes:properties::object as properties
        , attributes:external_id::string as external_id
        , attributes:organization::string as organization
        , attributes:phone_number::string as phone_number
        , attributes:last_event_date::timestamptz as last_event_date
        , attributes:location:zip::string as zip
        , attributes:location:city::string as city
        , attributes:location:region::string as region
        , attributes:location:country::string as country
        , attributes:location:address1::string as address1
        , attributes:location:address2::string as address2
        , attributes:location:latitude::string as latitude
        , attributes:location:timezone::string as timezone
        , attributes:location:longitude::string as longitude
        , attributes:subscriptions:sms:marketing:method::string as sms_marketing_method
        , attributes:subscriptions:sms:marketing:consent::string as sms_marketing_consent
        , attributes:subscriptions:sms:marketing:timestamp::timestamptz as sms_marketing_timestampt
        , attributes:subscriptions:sms:marketing:double_optin::string as sms_marketing_double_optin
        , attributes:subscriptions:sms:marketing:suppressions::string as sms_marketing_suppressions
        , attributes:subscriptions:sms:marketing:method_detail::string as sms_marketing_method_detail
        , attributes:subscriptions:sms:marketing:list_suppressions::string as sms_marketing_list_suppressions
        , attributes:subscriptions:sms:marketing:custom_method_detail::string as sms_marketing_custom_method_detail
        , attributes:subscriptions:email:marketing:method::string as email_marketing_method
        , attributes:subscriptions:email:marketing:consent::string as email_marketing_consent
        , attributes:subscriptions:email:marketing:timestamp::timestamptz as email_marketing_timestampt
        , attributes:subscriptions:email:marketing:double_optin::string as email_marketing_double_optin
        , attributes:subscriptions:email:marketing:suppressions::array as email_marketing_suppressions
        , attributes:subscriptions:email:marketing:method_detail::string as email_marketing_method_detail
        , attributes:subscriptions:email:marketing:list_suppressions::array as email_marketing_list_suppressions
        , attributes:subscriptions:email:marketing:custom_method_detail::string as email_marketing_custom_method_detail
        , updated::timestamptz as updated
    from source

)

select * from final