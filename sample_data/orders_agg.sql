-- Aggregates raw orders into a revenue summary per customer
INSERT INTO agg_revenue
SELECT
    o.customer_id,
    SUM(o.amount)                        AS total_revenue,
    COUNT(o.order_id)                    AS order_count,
    AVG(o.amount)                        AS avg_order_value,
    MAX(o.order_date)                    AS last_order_date,
    CAST(SUM(o.amount) AS DECIMAL(18,2)) AS total_revenue_dec
FROM raw_orders o
WHERE o.status = 'completed'
GROUP BY o.customer_id
