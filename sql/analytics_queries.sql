SELECT * FROM analytics.monthly_sales ORDER BY month;

SELECT product_name, category, units_sold, revenue
FROM analytics.top_products ORDER BY revenue DESC LIMIT 10;

SELECT segment, COUNT(*) AS customers, ROUND(AVG(lifetime_value),2) AS avg_lifetime_value
FROM analytics.customer_segments GROUP BY segment ORDER BY customers DESC;

SELECT * FROM analytics.payment_summary ORDER BY payment_method, payment_status;

SELECT c.city, COUNT(DISTINCT o.order_id) AS completed_orders,
       ROUND(SUM(i.quantity*i.unit_price),2) AS gross_sales
FROM raw_customers c JOIN raw_orders o ON o.customer_id=c.customer_id
JOIN raw_order_items i ON i.order_id=o.order_id
WHERE o.order_status='completed'
GROUP BY c.city ORDER BY gross_sales DESC;
