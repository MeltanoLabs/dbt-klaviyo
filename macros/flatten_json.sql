{%- macro flatten_json(table_name, array_column) -%}
    {{ return(adapter.dispatch('flatten_json')(table_name, array_column)) }}
{%- endmacro -%}

{%- macro default__flatten_json(table_name, array_column) -%}

{%- endmacro -%}

{%- macro snowflake__flatten_json(table_name, array_column) -%},
    lateral flatten(input => {{ table_name }}.{{ array_column }}) as flattened_table
{%- endmacro -%}

{%- macro redshift__flatten_json(table_name, array_column) -%}
    {# TODO - Untested #}
    {{ table_name }}.{{ array_column}}
{%- endmacro -%}
​
{%- macro bigquery__flatten_json(table_name, array_column) -%}
    {# TODO - Untested #}
    unnest({{ array_column }})
{%- endmacro -%}

​