-- Input views: fixture_orders, fixture_history, fixture_outbox.
-- Dates are prepared explicitly in Asia/Shanghai by each engine adapter.
-- metric: created
SELECT create_date AS biz_date, shop_id_snapshot AS shop_id,
       COUNT(*) AS orders, COUNT(DISTINCT user_id) AS users,
       SUM(pay_value_snapshot) AS known_cents,
       SUM(CASE WHEN pay_value_snapshot IS NULL THEN 1 ELSE 0 END) AS unknown_amount,
       SUM(CASE WHEN pay_value_snapshot=0 THEN 1 ELSE 0 END) AS free_orders
FROM fixture_orders GROUP BY create_date, shop_id_snapshot ORDER BY biz_date, shop_id;
-- metric: paid
SELECT h.event_date AS biz_date, o.shop_id_snapshot AS shop_id,
       COUNT(DISTINCT o.id) AS orders, SUM(o.pay_value_snapshot) AS known_cents,
       SUM(CASE WHEN o.pay_value_snapshot IS NULL THEN 1 ELSE 0 END) AS unknown_amount
FROM fixture_history h JOIN fixture_orders o ON h.order_id=o.id
WHERE h.event_type='ORDER_PAID'
GROUP BY h.event_date, o.shop_id_snapshot ORDER BY biz_date, shop_id;
-- metric: closed
SELECT event_date AS biz_date, COUNT(*) AS orders
FROM fixture_history WHERE event_type='ORDER_CLOSED'
GROUP BY event_date ORDER BY biz_date;
-- metric: status
SELECT status, COUNT(*) AS orders FROM fixture_orders GROUP BY status ORDER BY status;
-- metric: counts
SELECT (SELECT COUNT(*) FROM fixture_orders) AS orders,
       (SELECT COUNT(*) FROM fixture_outbox) AS outbox,
       (SELECT COUNT(*) FROM fixture_history) AS history,
       (SELECT COUNT(*) FROM fixture_outbox b LEFT JOIN fixture_history h ON b.id=h.event_id WHERE h.event_id IS NULL) AS missing_projection;
