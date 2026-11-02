-- Optional acceptance access to the dedicated synthetic database only.
-- No CREATE/INSERT/UPDATE/DELETE grants; does not change the application account.
GRANT SELECT ON hmdp_warehouse_fixture.* TO 'warehouse_reader'@'%';
