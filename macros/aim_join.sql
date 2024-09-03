-- File: macros/lineage_join.sql

{% macro lineage_join_old(left_model, right_model, join_type, join_on, left_alias='left', right_alias='right') %}

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


{% macro lineage_join(left_model, right_model, join_type, join_on, left_key, right_key, left_alias='left', right_alias='right') %}

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
