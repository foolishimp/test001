-- analyses/test_source.sql

SELECT *
FROM {{ ref('test001', 'orders') }}
LIMIT 10
