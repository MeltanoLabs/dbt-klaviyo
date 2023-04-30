{%- macro flatten_json_array(table_name, id_column, array_column) -%}
    {{ return(adapter.dispatch('flatten_json_array')(table_name, id_column, array_column)) }}
{%- endmacro -%}

{%- macro default__flatten_json_array(table_name, id_column, array_column) -%}
    select
        {{ id_column }}
        , json_array_elements({{ array_column }}::json) as {{ array_column }}
    from {{ table_name }}
{%- endmacro -%}

{%- macro snowflake__flatten_json_array(table_name, id_column, array_column) -%}
    select
        {{ id_column }}
        , flattened_table.key
        , flattened_table.value as {{ array_column }}
    from {{ table_name }},
    lateral flatten(input => {{ table_name }}.{{ array_column }}) as flattened_table
{%- endmacro -%}

{%- macro redshift__flatten_json_array(table_name, id_column, array_column) -%}
    {# TODO - Untested #}
    select
        {{ id_column }}
        , {{ array_column}}
    from
        {{ table_name }}
        , {{ table_name }}.{{ array_column}}
{%- endmacro -%}
​
​
{%- macro bigquery__flatten_json_array(table_name, id_column, array_column) -%}
    {# TODO - Untested #}
    select
        {{ id_column }}
        , arr_item as {{ array_column }}
    from
        {{ table_name }}
        , unnest({{ array_column }}) as arr_item

{%- endmacro -%}

​