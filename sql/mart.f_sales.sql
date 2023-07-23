-- ALTER TABLE mart.f_sales ADD COLUMN status VARCHAR(15);

DELETE FROM mart.f_sales WHERE date_id IN
(SELECT dc.date_id FROM staging.user_order_log AS uol
LEFT JOIN mart.d_calendar AS dc ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE = '{{ds}}');

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT dc.date_id, item_id,
        customer_id, city_id,
        CASE WHEN status='refunded' THEN -1*quantity ELSE quantity END AS quantity,
        CASE WHEN status='refunded' THEN -1*payment_amount ELSE payment_amount END AS payment_amount,
        CASE WHEN status IS NULL THEN 'shipped' ELSE status END AS status
FROM staging.user_order_log AS uol
JOIN mart.d_calendar AS dc ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE = '{{ds}}';