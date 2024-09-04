{% macro create_count_tokens_udf() %}
  {% set create_function_sql %}
    CREATE OR REPLACE FUNCTION count_tokens(input_string STRING)
    RETURNS INTEGER
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    HANDLER = 'count_tokens_py'
    AS 
$$
def count_tokens_py(input_string):
    return len(input_string) / 4
$$
  {% endset %}
  
  {% do run_query(create_function_sql) %}
{% endmacro %}