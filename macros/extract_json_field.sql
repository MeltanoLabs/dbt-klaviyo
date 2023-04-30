{%- macro extract_json_field(field_name, json_keys, target_name=target.type) -%}
    {{ return(adapter.dispatch('extract_json_field')(field_name, json_keys, target_name=target.type)) }}
{%- endmacro -%}

{%- macro default__extract_json_field(field_name, json_keys, target_name=target.type) -%}

    {%- if target_name is none -%}

    {%- elif target_name == 'snowflake' -%}

        {%- set field_array = field_name.split(":") -%}
        {%- set base = field_array[0] -%}
        {%- set other_fields_text = field_array[1:]|join(':') -%}

        {# https://docs.snowflake.com/en/sql-reference/functions/json_extract_path_text #}
        {# json_extract_path_text({{ field_name }}::variant, '{{ json_keys }}') #}

        {%- if other_fields_text == '' -%}
            json_extract_path_text({{ field_name }}::variant, '{{ json_keys }}')
        {%- else -%}
            json_extract_path_text({{ base }}::variant:{{ other_fields_text }}, '{{ json_keys }}')
        {%- endif -%}


    {%- elif target_name == 'bigquery' -%}
        {# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_query #}
    ​
        json_query({{ field_name }}::json, '$.{{ json_keys }}')

    {%- elif target_name == 'redshift' -%}
        {# https://docs.aws.amazon.com/redshift/latest/dg/json_extract_path_text.html #}
    ​
        json_extract_path_text(parse_json({{ field_name }}), '{{ json_keys }}')

    {%- elif target_name == 'postgres' -%}
        {# https://www.postgresql.org/docs/12/functions-json.html#}
    ​
        json_extract_path_text(parse_json({{ field_name }}), '{{ json_keys }}')

    {%- endif -%}
{%- endmacro %}

