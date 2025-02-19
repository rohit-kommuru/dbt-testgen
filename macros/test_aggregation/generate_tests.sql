

{% macro run_testgen(script_path, data_path, execute=True) %}
  {% set command = 'python ' ~ script_path ~ ' ' ~ data_path %}
  {{ log("Executing script: " ~ command, info=True) }}

  {% if execute %}
    {% do run_query('run-script', command) %}
  {% else %}
    {{ log("Skipping script execution", info=True) }}
  {% endif %}
{% endmacro %}

{% set data_path = var('data_path', 'default1') %}
{% set execute = var('execute', True) %}

{{ run_testgen('test_gen/dbt-testgen/testgen.py', data_path, execute) }}