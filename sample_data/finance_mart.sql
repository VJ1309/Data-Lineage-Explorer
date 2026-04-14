-- Finance mart: joins revenue summary with customer dimension
WITH customer_revenue AS (
    SELECT
        r.customer_id,
        r.total_revenue,
        r.order_count,
        r.avg_order_value
    FROM agg_revenue r
    WHERE r.total_revenue > 0
),
ranked AS (
    SELECT
        cr.customer_id,
        cr.total_revenue,
        cr.order_count,
        cr.avg_order_value,
        ROW_NUMBER() OVER (ORDER BY cr.total_revenue DESC) AS revenue_rank
    FROM customer_revenue cr
)
INSERT INTO mart_finance
SELECT
    r.customer_id,
    c.customer_name,
    c.region,
    r.total_revenue    AS revenue,
    r.order_count,
    r.avg_order_value,
    r.revenue_rank,
    r.total_revenue / NULLIF(r.order_count, 0) AS revenue_per_order
FROM ranked r
JOIN customer_dim c ON r.customer_id = c.customer_id
