CREATE USER IF NOT EXISTS 'canal'@'%' IDENTIFIED WITH mysql_native_password BY 'hmdp_canal_local_only';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
