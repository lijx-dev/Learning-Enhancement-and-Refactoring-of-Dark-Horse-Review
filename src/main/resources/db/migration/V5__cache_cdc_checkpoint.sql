CREATE TABLE tb_cache_cdc_checkpoint (
  destination VARCHAR(64) PRIMARY KEY,
  batch_id BIGINT NOT NULL,
  binlog_file VARCHAR(255) NOT NULL,
  binlog_offset BIGINT NOT NULL,
  event_time_ms BIGINT NOT NULL,
  processed_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
