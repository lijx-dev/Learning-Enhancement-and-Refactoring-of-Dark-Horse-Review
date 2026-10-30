ALTER TABLE tb_business_outbox
  ADD COLUMN lease_owner VARCHAR(64) NULL,
  ADD COLUMN lease_until TIMESTAMP(3) NULL,
  ADD COLUMN deliver_at TIMESTAMP(3) NULL,
  ADD COLUMN sent_at TIMESTAMP(3) NULL,
  ADD COLUMN consumed_at TIMESTAMP(3) NULL,
  ADD COLUMN consume_attempts INT NOT NULL DEFAULT 0,
  ADD COLUMN last_error VARCHAR(500) NULL,
  ADD INDEX idx_outbox_lease (state,lease_until,id);

CREATE TABLE tb_message_inbox (
  consumer_group VARCHAR(80) NOT NULL,
  event_id BIGINT NOT NULL,
  processed_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(consumer_group,event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_message_failure (
  consumer_group VARCHAR(80) NOT NULL,
  event_key VARCHAR(128) NOT NULL,
  event_id BIGINT NULL,
  broker_message_id VARCHAR(128) NULL,
  payload MEDIUMTEXT NOT NULL,
  reason VARCHAR(500) NOT NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(consumer_group,event_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_order_event_history (
  event_id BIGINT NOT NULL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  event_type VARCHAR(32) NOT NULL,
  occurred_at TIMESTAMP(3) NOT NULL,
  processed_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_order_events(order_id,event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE tb_voucher_order
  ADD COLUMN expire_at TIMESTAMP(3) NULL,
  ADD INDEX idx_order_deadline(status,voucher_type_snapshot,expire_at,id);
-- Backfill existing ordinary unpaid orders. A batched reconciler covers deadlines without a message.
UPDATE tb_voucher_order SET expire_at=TIMESTAMPADD(MINUTE,30,create_time)
WHERE status=1 AND voucher_type_snapshot=0;
