{%- macro say_hi(table_name, id_column, array_column) -%}
    {{ return(adapter.dispatch('say_hi')(table_name, id_column, array_column)) }}
{%- endmacro -%}

{%- macro default__say_hi(table_name, id_column, array_column) -%}
    {{print('hi from default')}}
{%- endmacro -%}

{%- macro redshift__say_hi(table_name, id_column, array_column) -%}
    {{print('hi from redshift')}}
{%- endmacro -%}

{%- macro snowflake__say_hi(table_name, id_column, array_column) -%}
    {{print('hi from snowflake')}}
{%- endmacro -%}

{%- macro bigquery__say_hi(table_name, id_column, array_column) -%}
    {{print('hi from bq')}}
{%- endmacro -%}
