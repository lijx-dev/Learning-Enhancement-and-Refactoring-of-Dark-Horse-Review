ALTER TABLE tb_business_outbox
  ADD COLUMN idempotency_key VARCHAR(100) NULL,
  ADD UNIQUE KEY uq_outbox_business_key(idempotency_key);
