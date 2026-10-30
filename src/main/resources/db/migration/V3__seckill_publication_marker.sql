ALTER TABLE tb_seckill_activity ADD COLUMN has_published TINYINT NOT NULL DEFAULT 0;
UPDATE tb_seckill_activity a SET has_published=1
WHERE EXISTS (SELECT 1 FROM tb_seckill_reservation r WHERE r.voucher_id=a.voucher_id);
