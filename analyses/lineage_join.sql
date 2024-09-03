{{ config(materialized='table') }}

{{ lineage_join(
    left_model='customer',
    right_model='orders',
    join_type='LEFT JOIN',
    join_on='c.customer_id = o.customer_id',
    left_key='customer_id',
    right_key='order_id',
    left_alias='c',
    right_alias='o'
) }}