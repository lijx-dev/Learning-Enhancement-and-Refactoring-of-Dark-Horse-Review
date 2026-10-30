ALTER TABLE tb_behavior_outbox ADD COLUMN sent_at TIMESTAMP(3) NULL;
UPDATE tb_behavior_outbox SET sent_at=created_at WHERE state='SENT';
