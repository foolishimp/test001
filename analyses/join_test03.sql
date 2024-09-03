-- File: analyses/join_test03.sql

{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('customer') }}
),
orders AS (
    SELECT * FROM {{ ref('orders') }}
)

SELECT 
    c.customer_id,
    c.name,
    o.order_id,
    o.order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id