-- analyses/lineage_group_by_examples.sql

-- Example 1: Combined Customer, Product, and Order Analysis
WITH customer_product_order_analysis AS (
    SELECT 
        customer_id, 
        customer_name, 
        product_category, 
        DATE_TRUNC('month', order_date) AS order_month,
        COUNT(order_id) AS order_count,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_revenue,
        AVG(total_price) AS average_order_value,
        ARRAY[
            ROW('order_id', ARRAY_AGG(DISTINCT order_id)),
            ROW('product_id', ARRAY_AGG(DISTINCT product_id)),
            ROW('customer_id', ARRAY_AGG(DISTINCT customer_id))
        ] AS _lineage
    FROM (
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
    ) AS base_subquery
    GROUP BY 
        customer_id, 
        customer_name, 
        product_category, 
        DATE_TRUNC('month', order_date)
)

-- Run the first analysis
SELECT * FROM customer_product_order_analysis;

-- Example 2: Product-focused Analysis
WITH product_analysis AS (
    SELECT 
        product_id, 
        product_name, 
        product_category, 
        DATE_TRUNC('month', order_date) AS order_month,
        COUNT(order_id) AS order_count,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_revenue,
        ARRAY[
            ROW('order_id', ARRAY_AGG(DISTINCT order_id)),
            ROW('product_id', ARRAY_AGG(DISTINCT product_id))
        ] AS _lineage
    FROM (
        SELECT 
            o.order_id,
            o.product_id,
            o.order_date,
            o.quantity,
            o.total_price,
            p.product_name,
            p.category AS product_category
        FROM {{ ref('orders') }} o
        LEFT JOIN {{ ref('products') }} p ON o.product_id = p.product_id
    ) AS base_subquery
    GROUP BY 
        product_id, 
        product_name, 
        product_category, 
        DATE_TRUNC('month', order_date)
)

-- Run the second analysis
SELECT * FROM product_analysis;