-- File: analyses/lineage_join_example.sql

{{ config(materialized='table') }}

-- Create sample tables (only in dev environment)
{% if target.name == 'dev' %}
    CREATE OR REPLACE TABLE customers AS (
        SELECT 1 AS customer_id, 'Alice' AS name
        UNION ALL SELECT 2, 'Bob'
        UNION ALL SELECT 3, 'Charlie'
    );

    CREATE OR REPLACE TABLE orders AS (
        SELECT 1 AS order_id, 1 AS customer_id, '2023-01-01' AS order_date
        UNION ALL SELECT 2, 1, '2023-01-15'
        UNION ALL SELECT 3, 2, '2023-02-01'
        UNION ALL SELECT 4, 3, '2023-02-15'
    );

    -- ... (create products and order_items tables similarly)
{% endif %}

-- Use the lineage_join macro
{% set customers_orders = lineage_join(
    left_model='customers',
    right_model='orders',
    join_type='LEFT JOIN',
    join_on='customers.customer_id = orders.customer_id',
    left_alias='c',
    right_alias='o'
) %}

-- ... (continue with more joins as needed)

-- Final select statement
SELECT 
    c__name AS customer_name,
    o__order_id,
    o__order_date,
    -- ... (other columns)
    _lineage
FROM {{ customers_orders }}
ORDER BY c__name, o__order_id