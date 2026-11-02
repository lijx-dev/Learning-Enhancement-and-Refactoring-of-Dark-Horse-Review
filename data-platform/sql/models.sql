-- Each section produces a named model. Source timestamps are UTC; business dates use Shanghai.
-- model: dim_shop_snapshot
SELECT id AS shop_id,name,type_id,area,address,avg_price,score FROM tb_shop;
-- model: dim_voucher_snapshot
SELECT id AS voucher_id,shop_id,title,CAST(pay_value AS DECIMAL(38,0)) AS current_price_cents,type,status FROM tb_voucher;
-- model: dwd_order_created
SELECT id AS order_id,user_id,voucher_id,COALESCE(shop_id_snapshot,'UNKNOWN') AS shop_id,
       to_timestamp(create_time) AS created_at_utc,
       to_date(from_utc_timestamp(to_timestamp(create_time),'Asia/Shanghai')) AS biz_date,
       CAST(pay_value_snapshot AS DECIMAL(38,0)) AS pay_cents,
       pay_value_snapshot IS NOT NULL AS amount_known,
       CAST(pay_value_snapshot AS DECIMAL(38,0))=0 AS is_free,
       voucher_type_snapshot AS voucher_type,
       'unknown' AS snapshot_provenance
FROM tb_voucher_order;
-- model: dwd_order_snapshot
SELECT id AS order_id,CAST(status AS INT) AS status,to_timestamp(pay_time) AS pay_time_utc,
       to_timestamp(use_time) AS use_time_utc,to_timestamp(refund_time) AS refund_time_utc,
       to_timestamp(expire_at) AS expire_at_utc FROM tb_voucher_order;
-- model: dwd_order_event
SELECT event_id,order_id,event_type,to_timestamp(occurred_at) AS occurred_at_utc,
       to_timestamp(processed_at) AS processed_at_utc,
       to_date(from_utc_timestamp(to_timestamp(occurred_at),'Asia/Shanghai')) AS event_date,
       CASE WHEN event_type='ORDER_CLOSED' THEN 'unknown' ELSE NULL END AS close_reason
FROM tb_order_event_history;
-- model: dim_date
SELECT biz_date,year(biz_date) AS year,month(biz_date) AS month,dayofmonth(biz_date) AS day,
       weekofyear(biz_date) AS iso_week FROM (
  SELECT biz_date FROM dwd_order_created UNION SELECT event_date FROM dwd_order_event
);
-- model: created_day
SELECT biz_date,shop_id,COUNT(*) AS created_orders,COUNT(DISTINCT user_id) AS created_users,
       SUM(pay_cents) AS created_known_cents,
       SUM(CASE WHEN amount_known THEN 0 ELSE 1 END) AS unknown_amount_orders,
       SUM(CASE WHEN is_free THEN 1 ELSE 0 END) AS free_orders
FROM dwd_order_created GROUP BY biz_date,shop_id;
-- model: paid_day
SELECT e.event_date AS biz_date,o.shop_id,COUNT(*) AS paid_orders,
       SUM(o.pay_cents) AS paid_known_cents,
       SUM(CASE WHEN o.amount_known THEN 0 ELSE 1 END) AS paid_unknown_amount_orders
FROM dwd_order_event e JOIN dwd_order_created o ON e.order_id=o.order_id
WHERE e.event_type='ORDER_PAID' GROUP BY e.event_date,o.shop_id;
-- model: closed_day
SELECT e.event_date AS biz_date,o.shop_id,COUNT(*) AS closed_events
FROM dwd_order_event e JOIN dwd_order_created o ON e.order_id=o.order_id
WHERE e.event_type='ORDER_CLOSED' GROUP BY e.event_date,o.shop_id;
-- model: dws_shop_order_day
SELECT k.biz_date,k.shop_id,COALESCE(c.created_orders,0) AS created_orders,
       COALESCE(c.created_users,0) AS created_users,
       CASE WHEN c.created_orders IS NULL THEN CAST(0 AS DECIMAL(38,0)) ELSE c.created_known_cents END AS created_known_cents,
       COALESCE(c.unknown_amount_orders,0) AS unknown_amount_orders,COALESCE(c.free_orders,0) AS free_orders,
       COALESCE(p.paid_orders,0) AS paid_orders,
       CASE WHEN p.paid_orders IS NULL THEN CAST(0 AS DECIMAL(38,0)) ELSE p.paid_known_cents END AS paid_known_cents,
       COALESCE(p.paid_unknown_amount_orders,0) AS paid_unknown_amount_orders,
       COALESCE(x.closed_events,0) AS closed_events
FROM (SELECT biz_date,shop_id FROM created_day UNION SELECT biz_date,shop_id FROM paid_day UNION SELECT biz_date,shop_id FROM closed_day) k
LEFT JOIN created_day c ON k.biz_date=c.biz_date AND k.shop_id=c.shop_id
LEFT JOIN paid_day p ON k.biz_date=p.biz_date AND k.shop_id=p.shop_id
LEFT JOIN closed_day x ON k.biz_date=x.biz_date AND k.shop_id=x.shop_id;
-- model: ads_shop_order_rank
SELECT *,dense_rank() OVER (PARTITION BY biz_date ORDER BY created_orders DESC) AS order_rank
FROM dws_shop_order_day;
-- model: ads_order_status
SELECT status,COUNT(*) AS orders FROM dwd_order_snapshot GROUP BY status;
-- model: ads_voucher_performance
SELECT o.biz_date,o.shop_id,o.voucher_id,COUNT(*) AS created_orders,
       SUM(o.pay_cents) AS created_known_cents,
       SUM(CASE WHEN o.amount_known THEN 0 ELSE 1 END) AS unknown_amount_orders,
       SUM(CASE WHEN s.status=4 THEN 1 ELSE 0 END) AS cancelled_as_of,
       SUM(CASE WHEN o.voucher_type='0' AND o.pay_cents>0 THEN 1 ELSE 0 END) AS paid_ordinary_cohort_orders,
       SUM(CASE WHEN o.voucher_type='0' AND o.pay_cents>0 AND s.status=4 THEN 1 ELSE 0 END) AS paid_ordinary_cancelled_as_of
FROM dwd_order_created o JOIN dwd_order_snapshot s ON o.order_id=s.order_id
GROUP BY o.biz_date,o.shop_id,o.voucher_id;
