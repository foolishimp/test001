-- macros/lineage_group_by.sql

{% macro lineage_group_by(base_model, group_by_columns, aggregations, lineage_keys) %}

{{ log("Debug: base_model = " ~ base_model, info=True) }}
{{ log("Debug: group_by_columns = " ~ group_by_columns, info=True) }}
{{ log("Debug: aggregations = " ~ aggregations, info=True) }}
{{ log("Debug: lineage_keys = " ~ lineage_keys, info=True) }}

SELECT 
    {% for column in group_by_columns %}
        {% set parts = column.split(' AS ') %}
        {% if parts|length == 2 %}
            {{ parts[0] }} AS {{ parts[1] }},
        {% else %}
            {{ column }},
        {% endif %}
    {% endfor %}
    {% for agg in aggregations %}
        {{ agg }},
    {% endfor %}
    ARRAY[
        {% for key in lineage_keys %}
            ROW('{{ key.split(".")[-1] }}', ARRAY_AGG(DISTINCT {{ key }}))
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    ] AS _lineage
FROM (
    {{ base_model }}
) AS base_subquery
GROUP BY 
    {% for column in group_by_columns %}
        {% set parts = column.split(' AS ') %}
        {{ parts[0] }}{% if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}