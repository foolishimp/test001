-- File: macros/lineage_join.sql

{% macro lineage_join_old2(left_model, right_model, join_type, join_on, left_alias='left', right_alias='right') %}

{% set left_cols = adapter.get_columns_in_relation(ref(left_model)) %}
{% set right_cols = adapter.get_columns_in_relation(ref(right_model)) %}

WITH left_cte AS (
    SELECT 
        *,
        '{{ left_model }}' AS _lineage_source
    FROM {{ ref(left_model) }}
),
right_cte AS (
    SELECT 
        *,
        '{{ right_model }}' AS _lineage_source
    FROM {{ ref(right_model) }}
)
SELECT 
    {% for col in left_cols %}
    {{ left_alias }}.{{ col.name }} AS {{ left_alias }}__{{ col.name }},
    {% endfor %}
    {% for col in right_cols %}
    {{ right_alias }}.{{ col.name }} AS {{ right_alias }}__{{ col.name }},
    {% endfor %}
    CASE 
        WHEN {{ left_alias }}._lineage_source IS NOT NULL AND {{ right_alias }}._lineage_source IS NOT NULL 
        THEN {{ left_alias }}._lineage_source || ',' || {{ right_alias }}._lineage_source
        WHEN {{ left_alias }}._lineage_source IS NOT NULL 
        THEN {{ left_alias }}._lineage_source
        ELSE {{ right_alias }}._lineage_source
    END AS _lineage
FROM left_cte AS {{ left_alias }}
{{ join_type }} right_cte AS {{ right_alias }}
    ON {{ join_on }}

{% endmacro %}


{% macro lineage_join_old(left_model, right_model, join_type, join_on, left_key, right_key, left_alias='left', right_alias='right') %}

{% set left_cols = adapter.get_columns_in_relation(ref(left_model)) %}
{% set right_cols = adapter.get_columns_in_relation(ref(right_model)) %}

WITH left_cte AS (
    SELECT 
        *,
        '{{ left_model }}:' || {{ left_key }} AS _lineage_source
    FROM {{ ref(left_model) }}
),
right_cte AS (
    SELECT 
        *,
        '{{ right_model }}:' || {{ right_key }} AS _lineage_source
    FROM {{ ref(right_model) }}
)
SELECT 
    {% for col in left_cols %}
    {{ left_alias }}.{{ col.name }} AS {{ left_alias }}__{{ col.name }},
    {% endfor %}
    {% for col in right_cols %}
    {{ right_alias }}.{{ col.name }} AS {{ right_alias }}__{{ col.name }},
    {% endfor %}
    CASE 
        WHEN {{ left_alias }}._lineage_source IS NOT NULL AND {{ right_alias }}._lineage_source IS NOT NULL 
        THEN {{ left_alias }}._lineage_source || ',' || {{ right_alias }}._lineage_source
        WHEN {{ left_alias }}._lineage_source IS NOT NULL 
        THEN {{ left_alias }}._lineage_source
        ELSE {{ right_alias }}._lineage_source
    END AS _lineage
FROM left_cte AS {{ left_alias }}
{{ join_type }} right_cte AS {{ right_alias }}
    ON {{ join_on }}

{% endmacro %}


{% macro lineage_join_slow(base_model, joins) %}

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
{% set base_columns = adapter.get_columns_in_relation(ref(base_model_name)) %}
{% for col in base_columns %}
    {{ base_model_alias }}.{{ col.name }} AS {{ base_model_alias }}__{{ col.name }},
{% endfor %}
{% for join in joins %}
    {% set join_parts = join.split(',') %}
    {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
    {% set join_columns = adapter.get_columns_in_relation(ref(join_model)) %}
    {% for col in join_columns %}
    {{ join_alias }}.{{ col.name }} AS {{ join_alias }}__{{ col.name }}{% if not loop.last or not loop.parent.last %},{% endif %}
    {% endfor %}
{% endfor %},
    CASE 
        WHEN {{ base_model_alias }}._lineage_source IS NOT NULL 
        THEN {{ base_model_alias }}._lineage_source
        {% for join in joins %}
            {% set join_parts = join.split(',') %}
            {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
        WHEN {{ join_alias }}._lineage_source IS NOT NULL 
        THEN CONCAT_WS(',', 
            {{ base_model_alias }}._lineage_source, 
            {% for prev_join in joins[:loop.index] %}
                {% set prev_join_parts = prev_join.split(',') %}
                {% set prev_join_alias = prev_join_parts[3] %}
                {{ prev_join_alias }}._lineage_source{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        {% endfor %}
    END AS _lineage
FROM base_cte AS {{ base_model_alias }}
{% for join in joins %}
    {% set join_parts = join.split(',') %}
    {% set join_type, join_model, join_key, join_alias, join_condition = join_parts %}
{{ join_type }} {{ join_alias }}_cte AS {{ join_alias }}
    ON {{ join_condition }}
{% endfor %}

{% endmacro %}
