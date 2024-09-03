-- Example usage of lineage_join macro with customer, orders, and products tables

{% set base_model = "customer,customer_id,c" %}
{% set joins = [
    "LEFT JOIN,orders,order_id,o,c.customer_id = o.customer_id",
    "LEFT JOIN,products,product_id,p,o.product_id = p.product_id"
] %}
{% set columns = "
    c.customer_id,
    c.name,
    c.email,
    c.city,
    o.order_id,
    o.order_date,
    o.quantity,
    p.product_name,
    p.price,
    p.category,
    o.total_price
" %}

{{ lineage_join(base_model, joins, columns) }}