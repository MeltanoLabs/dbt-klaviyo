{% set postgres_columns =
    [
        'id as campaign_id'
        , 'updated_at::timestamp'
        , 'attributes::json->''name'' as campaign_name'
        , 'attributes::json->''type'' as type'
        , 'attributes::json->''status'' as status'
        , 'attributes::json->''channel'' as channel'
        , 'attributes::json->''message'' as message'
        , 'attributes::json->''archived'' as is_archived'
        , 'attributes::json->''send_time'' as send_time'
        , 'attributes::json->''created_at'' as created_at'
        , 'attributes::json->''scheduled_at'' as scheduled_at'
    ]
%}

{% set snowflake_columns =
    [
        'id as campaign_id'
        , 'updated_at::timestamp'
        , 'attributes:name as campaign_name'
        , 'attributes:type as type'
        , 'attributes:status as status'
        , 'attributes:channel as channel'
        , 'attributes:message as message'
        , 'attributes:archived as is_archived'
        , 'attributes:send_time as send_time'
        , 'attributes:created_at as created_at'
        , 'attributes:scheduled_at as scheduled_at'
    ]
%}



with source as (

    select * from {{ source('snowflake_klaviyo', 'campaigns') }}

)

, final as (
    select
        {% if target.type == 'postgres'%}

            {% for column in postgres_columns -%}
                {%- if loop.first is true %}
                    {{ column }}
                {% else %}
                    , {{ column }}
                {% endif -%}
            {% endfor %}

        {% elif target.type == 'snowflake'%}

            {% for column in snowflake_columns -%}
                {%- if loop.first is true %}
                    {{ column }}
                {% else %}
                    , {{ column }}
                {% endif -%}
            {% endfor %}

        {% else %}

            select *

        {% endif %}

    from source
)

select * from final