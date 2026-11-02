"""Validate Spark SQL, Python workers, Parquet and read-only project JDBC access."""
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType


def main():
    output = Path('/workspace/target/local-environment/spark')
    output.mkdir(parents=True, exist_ok=True)
    (output / 'verification.json').unlink(missing_ok=True)
    spark = (
        SparkSession.builder.appName('hmdp-offline-environment-smoke')
        .config('spark.sql.session.timeZone', 'Asia/Shanghai')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    try:
        schema = StructType([
            StructField('order_id', LongType(), False),
            StructField('shop_id', LongType(), False),
            StructField('amount', DecimalType(18, 2), False),
            StructField('biz_date', StringType(), False),
        ])
        # Includes a Java Long beyond JavaScript's exact integer range.
        rows = [
            (9007199254740993, 1, Decimal('12.34'), '2026-10-30'),
            (9007199254740994, 1, Decimal('0.66'), '2026-10-30'),
            (9007199254740995, 2, Decimal('5.00'), '2026-10-31'),
        ]
        spark.createDataFrame(rows, schema).createOrReplaceTempView('orders')
        aggregate = spark.sql('SELECT shop_id, SUM(amount) amount FROM orders GROUP BY shop_id')
        assert {row.shop_id: str(row.amount) for row in aggregate.collect()} == {1: '13.00', 2: '5.00'}
        for _ in range(2):
            spark.table('orders').write.mode('overwrite').partitionBy('biz_date').parquet(str(output / 'parquet'))
            restored = spark.read.parquet(str(output / 'parquet'))
            assert restored.count() == 3
            assert restored.selectExpr('min(order_id) id').first().id == 9007199254740993
        # serverTimezone describes the server to the legacy driver; it does not SET time_zone.
        # Match the actual JDBC session to that declaration, then check epoch conversion.
        jdbc_options = {
            'url': 'jdbc:mysql://mysql:3306/hmdp?useSSL=false&serverTimezone=Asia/Shanghai',
            'driver': 'com.mysql.jdbc.Driver',
            'user': 'warehouse_reader',
            'password': 'warehouse_read_local_only',
            'sessionInitStatement': "SET time_zone = '+08:00'",
        }
        clock = (
            spark.read.format('jdbc').options(**jdbc_options)
            .option('dbtable', '(SELECT NOW() source_now, UNIX_TIMESTAMP() epoch_seconds, '
                    'TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) offset_seconds) clock')
            .load()
        )
        clock_row = clock.selectExpr(
            'CAST(source_now AS BIGINT) decoded_epoch', 'epoch_seconds', 'offset_seconds'
        ).first()
        assert clock_row.offset_seconds == 28800, clock_row
        assert abs(clock_row.decoded_epoch - clock_row.epoch_seconds) <= 1, clock_row
        snapshot = (
            spark.read.format('jdbc')
            .options(**jdbc_options)
            .option('dbtable', '(SELECT id, name, type_id FROM tb_shop) shops')
            .load()
        )
        source_count = snapshot.count()
        assert source_count > 0
        snapshot.write.mode('overwrite').parquet(str(output / 'mysql-shops'))
        assert spark.read.parquet(str(output / 'mysql-shops')).count() == source_count
        report = {
            'verified_at': datetime.now(timezone.utc).isoformat(),
            'passed': True,
            'spark_version': spark.version,
            'java_version': spark._jvm.java.lang.System.getProperty('java.version'),
            'python_version': platform.python_version(),
            'master': spark.sparkContext.master,
            'mysql_shop_rows': source_count,
            'spark_session_timezone': spark.conf.get('spark.sql.session.timeZone'),
            'jvm_timezone': spark._jvm.java.util.TimeZone.getDefault().getID(),
            'jdbc_session_offset_seconds': clock_row.offset_seconds,
            'checks': ['Python worker serialization', 'SQL decimal aggregation', 'Long precision',
                       'partitioned Parquet roundtrip', 'repeat overwrite without duplicate rows',
                       'read-only MySQL JDBC to Parquet', 'JDBC timestamp epoch and session timezone'],
            'limits': 'Environment smoke only; not a warehouse pipeline, distributed test or consistent multi-table snapshot.',
        }
        (output / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print('ENVIRONMENT_SMOKE=' + json.dumps(report))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
