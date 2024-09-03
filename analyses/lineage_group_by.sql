-- analyses/lineage_group_by.sql

{% set base_model %}
    SELECT 
        o.customer_id,
        o.order_id,
        o.product_id,
        o.order_date,
        o.quantity,
        o.total_price,
        c.name AS customer_name,
        p.category AS product_category
    FROM {{ ref('orders') }} o
    LEFT JOIN {{ ref('customer') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('products') }} p ON o.product_id = p.product_id
{% endset %}

{% set group_by_columns = [
    "customer_id",
    "customer_name",
    "product_category",
    "DATE_TRUNC('month', order_date) AS order_month"
] %}

{% set aggregations = [
    "COUNT(order_id) AS order_count",
    "SUM(quantity) AS total_quantity",
    "SUM(total_price) AS total_revenue",
    "AVG(total_price) AS average_order_value"
] %}

{% set lineage_keys = [
    "order_id",
    "product_id",
    "customer_id"
] %}

{{ lineage_group_by(base_model, group_by_columns, aggregations, lineage_keys) }}


