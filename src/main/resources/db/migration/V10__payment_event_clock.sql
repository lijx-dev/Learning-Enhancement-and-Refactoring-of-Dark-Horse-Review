-- created_at remains the business event instant used by existing consumers.
-- Keep enqueue time separate, including for delayed events and Feed continuations.
ALTER TABLE tb_voucher_order MODIFY pay_time TIMESTAMP(3) NULL DEFAULT NULL COMMENT '支付业务发生时间';
ALTER TABLE tb_business_outbox ADD COLUMN enqueued_at TIMESTAMP(3) NULL DEFAULT NULL COMMENT '入队时间；迁移前记录以created_at近似回填';
UPDATE tb_business_outbox SET enqueued_at=created_at;
ALTER TABLE tb_business_outbox MODIFY enqueued_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '入队时间；迁移前记录以created_at近似回填';
