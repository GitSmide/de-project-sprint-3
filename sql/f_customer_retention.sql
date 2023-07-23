/*
CREATE TABLE mart.f_customer_retention(
new_customers_count INT,
returning_customers_count INT,
refunded_customer_count INT,
period_name VARCHAR(10),
period_id varchar(100),
item_id INT,
new_customers_revenue NUMERIC(14,2),
returning_customers_revenue NUMERIC(14,2),
customers_refunded INT,
CONSTRAINT f_customer_item_id_and_period_id_unique UNIQUE (item_id, period_id)
);
*/
INSERT INTO mart.f_customer_retention (
new_customers_count,
returning_customers_count,
refunded_customer_count,
period_name,
period_id,
item_id,
new_customers_revenue,
returning_customers_revenue,
customers_refunded
)
WITH new_part AS
(
SELECT customer_id,
item_id,
status,
COUNT(*) as count_orders,
SUM(payment_amount) as revenue
FROM staging.user_order_log
WHERE date_time BETWEEN ('{{ds}}'::DATE-7) AND ('{{ds}}'::DATE-1)
GROUP BY customer_id, item_id, status
),
new_client AS
(
SELECT item_id,
       COUNT(customer_id) AS new_customers_count,
       SUM(revenue) AS new_customers_revenue
FROM new_part
WHERE status='shipped' AND count_orders=1
GROUP BY item_id
),
old_client AS
(
SELECT item_id,
       COUNT(customer_id) AS returning_customers_count,
       SUM(revenue) AS returning_customers_revenue
FROM new_part
WHERE status='shipped' AND count_orders>1
GROUP BY item_id 
),
ref_client AS
(
SELECT item_id,
       COUNT(customer_id) AS refunded_customer_count,
       SUM(revenue) AS customers_refunded
FROM new_part
WHERE status='refunded'
GROUP BY item_id
),
ib_table AS
(
SELECT DISTINCT item_id
FROM  new_part
)
SELECT
nc.new_customers_count,
oc.returning_customers_count,
rc.refunded_customer_count,
'weekly' as period_name,
(('{{ds}}'::DATE-7)::VARCHAR(20) || ' - ' || ('{{ds}}'::DATE-1)::VARCHAR(20)) AS period_id,
it.item_id,
nc.new_customers_revenue,
oc.returning_customers_revenue,
rc.customers_refunded
FROM ib_table AS it
LEFT JOIN new_client AS nc USING (item_id)
LEFT JOIN old_client AS oc USING (item_id)
LEFT JOIN ref_client AS rc USING (item_id)
ON conflict(item_id, period_id) do UPDATE SET
(
new_customers_count,
returning_customers_count,
refunded_customer_count,
period_name,
new_customers_revenue,
returning_customers_revenue,
customers_refunded
)=(
excluded.new_customers_count,
excluded.returning_customers_count,
excluded.refunded_customer_count,
excluded.period_name,
excluded.new_customers_revenue,
excluded.returning_customers_revenue,
excluded.customers_refunded
);