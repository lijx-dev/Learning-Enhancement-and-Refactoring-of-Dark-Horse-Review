-- Local-only demonstration account: analytics can read source facts, never mutate them.
CREATE USER IF NOT EXISTS 'warehouse_reader'@'%' IDENTIFIED BY 'warehouse_read_local_only';
GRANT SELECT ON hmdp.* TO 'warehouse_reader'@'%';
