ALTER TABLE tb_voucher_order ADD COLUMN shop_id_snapshot BIGINT NULL;
UPDATE tb_voucher_order o JOIN tb_voucher v ON v.id=o.voucher_id SET o.shop_id_snapshot=v.shop_id;

CREATE TABLE tb_behavior_outbox (
  event_id VARCHAR(64) PRIMARY KEY,
  shop_id BIGINT NOT NULL,
  payload TEXT NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  lease_owner VARCHAR(64) NULL,
  lease_until TIMESTAMP(3) NULL,
  last_error VARCHAR(500) NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_behavior_due(state,next_attempt_at,event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_behavior_event (
  event_id VARCHAR(64) PRIMARY KEY,
  shop_id BIGINT NOT NULL,
  user_id BIGINT NULL,
  action VARCHAR(16) NOT NULL,
  occurred_at_ms BIGINT NOT NULL,
  bucket_ms BIGINT NOT NULL,
  KEY idx_behavior_time(bucket_ms,shop_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_shop_heat_bucket (
  shop_id BIGINT NOT NULL,
  bucket_ms BIGINT NOT NULL,
  visits BIGINT NOT NULL DEFAULT 0,
  orders BIGINT NOT NULL DEFAULT 0,
  payments BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY(shop_id,bucket_ms),
  KEY idx_heat_time(bucket_ms,shop_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_shop_visit_user (
  shop_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  bucket_ms BIGINT NOT NULL,
  PRIMARY KEY(shop_id,user_id,bucket_ms),
  KEY idx_visit_user_time(bucket_ms,shop_id,user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_behavior_failure (
  topic VARCHAR(100) NOT NULL,
  partition_id INT NOT NULL,
  message_offset BIGINT NOT NULL,
  payload TEXT NOT NULL,
  reason VARCHAR(500) NOT NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(topic,partition_id,message_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
