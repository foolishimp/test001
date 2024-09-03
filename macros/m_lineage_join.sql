{% macro lineage_join(base_model, joins, columns='*') %}

{{ log("Debug: base_model = " ~ base_model, info=True) }}
{{ log("Debug: joins = " ~ joins, info=True) }}
{{ log("Debug: columns = " ~ columns, info=True) }}

{% set base_model_parts = base_model.split(',') %}
{% set base_model_name, base_model_key, base_model_alias = base_model_parts %}

WITH base_cte AS (
    SELECT 
        *,
        '{{ base_model_name }}:' || {{ base_model_key }} AS _lineage_source
    FROM {{ ref(base_model_name) }}
)

{% for join in joins %}
    {% set join_parts = join.split(',') %}
    {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
, {{ join_alias }}_cte AS (
    SELECT 
        *,
        '{{ join_model }}:' || {{ join_key }} AS _lineage_source
    FROM {{ ref(join_model) }}
)
{% endfor %}

SELECT 
{% if columns == '*' %}
    base.*,
    {% for join in joins %}
        {% set join_parts = join.split(',') %}
        {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
        {{ join_alias }}.*{% if not loop.last %},{% endif %}
    {% endfor %}
{% else %}
    {{ columns }},
{% endif %}
    ARRAY[{{ base_model_alias }}._lineage_source
    {% for join in joins %}
        {% set join_parts = join.split(',') %}
        {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
        , {{ join_alias }}._lineage_source
    {% endfor %}
    ] AS _lineage
FROM base_cte AS {{ base_model_alias }}
{% for join in joins %}
    {% set join_parts = join.split(',') %}
    {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
    {{ join_type }} {{ join_alias }}_cte AS {{ join_alias }}
        ON {{ join_condition }}
{% endfor %}
GROUP BY 
{% if columns == '*' %}
    base.*,
    {% for join in joins %}
        {% set join_parts = join.split(',') %}
        {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
        {{ join_alias }}.*{% if not loop.last %},{% endif %}
    {% endfor %}
{% else %}
    {{ columns }}
{% endif %}
    , {{ base_model_alias }}._lineage_source
    {% for join in joins %}
        {% set join_parts = join.split(',') %}
        {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
        , {{ join_alias }}._lineage_source
    {% endfor %}

{% endmacro %}